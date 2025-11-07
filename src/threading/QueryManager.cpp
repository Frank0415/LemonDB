#include "QueryManager.h"

#include <chrono>
#include <exception>
#include <iostream>
#include <sstream>
#include <thread>

#include "OutputPool.h"
#include "db/QueryBase.h"
#include "query/QueryResult.h"

QueryManager::~QueryManager()
{
  shutdown();
}

void QueryManager::addQuery(size_t query_id, const std::string& table_name, Query* query_ptr)
{
  if (nullptr == query_ptr)
  {
    return;
  }

  // Update query counter
  query_counter.fetch_add(1);

  {
    std::lock_guard lock(table_map_mutex);

    // Create table structures if this is the first query for this table
    if (!table_query_map.contains(table_name))
    {
      table_query_map[table_name] = std::deque<QueryEntry>();
      table_query_sem[table_name] = std::make_unique<std::counting_semaphore<>>(0);

      // Spawn a per-table execution thread
      table_threads.emplace_back(executeQueryForTable, this, table_name);

      // std::cerr << "[QueryManager] Created execution thread for table: " << table_name << "\n";
    }

    // Enqueue query with its ID
    table_query_map[table_name].push_back({query_id, query_ptr});

    // std::cerr << "[QueryManager] Queued query " << query_id << " for table '" << table_name
    //           << "' (queue size: " << table_query_map[table_name].size() << ")\n";
  }

  // Signal the semaphore to wake up the table's execution thread
  table_query_sem[table_name]->release();
}

void QueryManager::setExpectedQueryCount(size_t count)
{
  expected_query_count.store(count);
  // std::cerr << "[QueryManager] Expected query count set to: " << count << "\n";
}

void QueryManager::waitForCompletion()
{
  const size_t expected = expected_query_count.load();
  // std::cerr << "[QueryManager] Waiting for " << expected << " queries to complete...\n";

  constexpr int poll_interval_ms = 10;

  // Wait until all queries have been executed and results added to OutputPool
  while (completed_query_count.load() < expected)
  {
    // std::cerr << "[QueryManager] Progress: " << completed_query_count.load() << "/" << expected
    //           << " queries completed\n";
    std::this_thread::sleep_for(std::chrono::milliseconds(poll_interval_ms));
  }

  // std::cerr << "[QueryManager] All " << expected << " queries completed!\n";
  // std::cerr << "[QueryManager] Signaling all table threads to shutdown\n";

  // Signal shutdown flag
  is_end.store(true);

  // Release all semaphores to wake up table threads so they can exit
  {
    std::lock_guard lock(table_map_mutex);
    for (auto& sem_pair : table_query_sem)
    {
      // std::cerr << "[QueryManager] Releasing semaphore for table: " << sem_pair.first << "\n";
      sem_pair.second->release();
    }
  }

  // Join all table execution threads
  // std::cerr << "[QueryManager] Joining " << table_threads.size() << " table threads\n";
  for (auto& thread : table_threads)
  {
    if (thread.joinable())
    {
      // std::cerr << "[QueryManager] Joining a table thread...\n";
      thread.join();
      // std::cerr << "[QueryManager] Table thread joined\n";
    }
  }

  // std::cerr << "[QueryManager] All table threads completed\n";
}

void QueryManager::shutdown()
{
  is_end.store(true);

  // Release all semaphores to wake up threads
  {
    std::lock_guard lock(table_map_mutex);
    for (auto& sem_pair : table_query_sem)
    {
      sem_pair.second->release();
    }
  }
}

void QueryManager::executeQueryForTable(QueryManager* manager, const std::string& table_name)
{
  // std::cerr << "[QueryManager] Table thread started for '" << table_name << "'\n";

  while (!manager->is_end.load())
  {
    // Block until a query is available for this table
    manager->table_query_sem[table_name]->acquire();

    // Check if shutdown was signaled
    if (manager->is_end.load())
    {
      // std::cerr << "[QueryManager] Table '" << table_name << "' thread exiting (shutdown)\n";
      break;
    }

    // Get the next query for this table
    QueryEntry query_entry{};
    {
      std::lock_guard lock(manager->table_map_mutex);

      auto& queue = manager->table_query_map[table_name];
      if (queue.empty())
      {
        // Spurious wakeup or shutdown - continue
        // std::cerr << "[QueryManager] WARNING: Queue empty after acquire for '" << table_name
        //           << "' - spurious wakeup\n";
        continue;
      }

      query_entry = queue.front();
      queue.pop_front();

      // std::cerr << "[QueryManager] Dequeued query " << query_entry.query_id << " from table '"
      //           << table_name << "', remaining in queue: " << queue.size() << "\n";
    }

    const size_t query_id = query_entry.query_id;
    Query* query_ptr = query_entry.query_ptr;

    // std::cerr << "[QueryManager] Executing query " << query_id << " for table '" << table_name
    //           << "'\n";

    // Execute the query
    std::string result_str;
    bool is_wait_query = false;
    try
    {
      QueryResult::Ptr result = query_ptr->execute();

      std::ostringstream oss;
      if (result && result->display())
      {
        oss << *result;
      }
      result_str = oss.str();

