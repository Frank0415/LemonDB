#include "QueryManager.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <deque>
#include <exception>
#include <iostream>
#include <memory>
#include <mutex>
#include <semaphore>
#include <sstream>
#include <string>
#include <thread>

#include "OutputPool.h"
#include "db/QueryBase.h"
#include "query/QueryResult.h"

namespace {
bool isWaitQueryException(const std::exception &exc) {
  return std::string(exc.what()).find("WaitQuery completed") !=
         std::string::npos;
}

std::string formatQueryResult(const QueryResult::Ptr &result) {
  if (result && result->display()) {
    std::ostringstream oss;
    oss << *result;
    return oss.str();
  }
  return "";
}

std::string formatErrorMessage(const std::exception &exc) {
  std::ostringstream oss;
  oss << "Error: " << exc.what() << "\n";
  return oss.str();
}
}  // namespace

QueryManager::~QueryManager() { shutdown(); }

void QueryManager::addQuery(size_t query_id, const std::string &table_name,
                            Query *query_ptr) {
  if (nullptr == query_ptr) {
    return;
  }
  // std::cerr << "Adding query for number " << query_counter << "\n";
  // Update query counter
  query_counter.fetch_add(1);

  {
    const std::scoped_lock lock(table_map_mutex);

    // Create table structures if this is the first query for this table
    if (!table_query_map.contains(table_name)) {
      createTableStructures(table_name);
    }

    // Enqueue query with its ID
    table_query_map[table_name].push_back({query_id, query_ptr});
  }

  // Signal the semaphore to wake up the table's execution thread
  // Must hold lock while accessing semaphore
  table_query_sem[table_name]->release();
}

void QueryManager::addImmediateResult(size_t query_id,
                                      const std::string &result) {
  output_pool.addResult(query_id, result);
  completed_query_count.fetch_add(1);
}

void QueryManager::setExpectedQueryCount(size_t count) {
  expected_query_count.store(count);
}

void QueryManager::waitForCompletion() {
  const size_t expected = expected_query_count.load();

  constexpr int poll_interval_ms = 5;

  // Wait until all queries have been executed and results added to OutputPool
  while (completed_query_count.load() < expected) {

    std::this_thread::sleep_for(std::chrono::milliseconds(poll_interval_ms));
  }

  // Signal shutdown flag
  is_end.store(true);

  releaseSemaphores();
  joinThreads();
}

void QueryManager::shutdown() {
  is_end.store(true);
  releaseSemaphores();
}

void QueryManager::createTableStructures(const std::string &table_name) {
  table_query_map[table_name] = std::deque<QueryEntry>();
  table_query_sem[table_name] = std::make_unique<std::counting_semaphore<>>(0);

  // Spawn a per-table execution thread
  table_threads.emplace_back(executeQueryForTable, this, table_name);
}

void QueryManager::releaseSemaphores() {
  const std::scoped_lock lock(table_map_mutex);
  for (auto &sem_pair : table_query_sem) {
    sem_pair.second->release();
  }
}

void QueryManager::joinThreads() {
  for (auto &thread : table_threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}

void QueryManager::executeQueryForTable(QueryManager *manager,
                                        const std::string &table_name) {

  while (!manager->is_end.load()) {
    std::counting_semaphore<> *sem_ptr =
        manager->getSemaphoreForTable(table_name);
    if (sem_ptr == nullptr) {
      break;  // Table was removed
    }

    // Acquire semaphore outside lock to avoid deadlock
    sem_ptr->acquire();

    // Check if shutdown was signaled after acquiring
    if (manager->is_end.load()) {
      break;
    }

    std::optional<QueryEntry> query_entry = manager->dequeueQuery(table_name);
    if (!query_entry.has_value()) {
      continue;  // Spurious wakeup or shutdown
    }

    manager->executeAndStoreResult(query_entry.value());
  }
}

std::counting_semaphore<> *
QueryManager::getSemaphoreForTable(const std::string &table_name) {
  const std::scoped_lock lock(table_map_mutex);
  auto sem_iter = table_query_sem.find(table_name);
  if (sem_iter == table_query_sem.end()) {
    return nullptr;
  }
  return sem_iter->second.get();
}

std::optional<QueryManager::QueryEntry>
QueryManager::dequeueQuery(const std::string &table_name) {
  const std::scoped_lock lock(table_map_mutex);

  auto &queue = table_query_map[table_name];
  if (queue.empty()) {
    return std::nullopt;
  }

  QueryEntry entry = queue.front();
  queue.pop_front();
  return entry;
}

void QueryManager::executeAndStoreResult(const QueryEntry &query_entry) {
  const size_t query_id = query_entry.query_id;
  std::unique_ptr<Query> query_ptr(query_entry.query_ptr);

  std::string result_str;
  bool is_wait_query = false;

  try {
    QueryResult::Ptr result = query_ptr->execute();
    result_str = formatQueryResult(result);
  } catch (const std::exception &exc) {
    if (isWaitQueryException(exc)) {
      is_wait_query = true;
    } else {
      result_str = formatErrorMessage(exc);
    }
  }

  // Only store result and increment count if it's not a WaitQuery
  if (!is_wait_query) {
    output_pool.addResult(query_id, result_str);
    completed_query_count.fetch_add(1);
  }
}

bool QueryManager::isComplete() const {
  return completed_query_count.load(std::memory_order_acquire) >=
         expected_query_count.load(std::memory_order_acquire);
}

size_t QueryManager::getCompletedQueryCount() const {
  return completed_query_count.load(std::memory_order_acquire);
}

size_t QueryManager::getExpectedQueryCount() const {
  return expected_query_count.load(std::memory_order_acquire);
}
