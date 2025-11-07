#include "QueryManager.h"

#include <chrono>
#include <cstddef>
#include <exception>
#include <sstream>
#include <string>
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
    }

    // Enqueue query with its ID
    table_query_map[table_name].push_back({query_id, query_ptr});
  }

  // Signal the semaphore to wake up the table's execution thread
  // Must hold lock while accessing semaphore
  table_query_sem[table_name]->release();
}

void QueryManager::setExpectedQueryCount(size_t count)
{
  expected_query_count.store(count);
}

void QueryManager::waitForCompletion()
{
  const size_t expected = expected_query_count.load();

  constexpr int poll_interval_ms = 5;

  // Wait until all queries have been executed and results added to OutputPool
  while (completed_query_count.load() < expected)
  {

    std::this_thread::sleep_for(std::chrono::milliseconds(poll_interval_ms));
  }

  // Signal shutdown flag
  is_end.store(true);

  // Release all semaphores to wake up threads
  {
    std::lock_guard lock(table_map_mutex);
    for (auto& sem_pair : table_query_sem)
    {
      sem_pair.second->release();
    }
  }

  // Join all table execution threads

  for (auto& thread : table_threads)
  {
    if (thread.joinable())
    {
      thread.join();
    }
  }
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

  while (!manager->is_end.load())
  {
    // Get semaphore pointer while holding lock to avoid race
    std::counting_semaphore<>* sem_ptr = nullptr;
    {
      std::lock_guard lock(manager->table_map_mutex);
      auto sem_iter = manager->table_query_sem.find(table_name);
      if (sem_iter == manager->table_query_sem.end())
      {
        // Table was removed, exit
        break;
      }
      sem_ptr = sem_iter->second.get();
    }

    // Acquire semaphore outside lock to avoid deadlock
    sem_ptr->acquire();

    // Check if shutdown was signaled after acquiring
    if (manager->is_end.load())
    {

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

        continue;
      }

      query_entry = queue.front();
      queue.pop_front();
    }

    const size_t query_id = query_entry.query_id;
    std::unique_ptr<Query> query_ptr(query_entry.query_ptr);
    query_entry.query_ptr = nullptr;

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
    }
    catch (const std::exception& exc)
    {
      // Check if this is a WaitQuery completion exception
      if (std::string(exc.what()).find("WaitQuery completed") != std::string::npos)
      {
        // WaitQuery completed - don't add to OutputPool

        is_wait_query = true;
      }
      else
      {
        std::ostringstream oss;
        oss << "Error: " << exc.what() << "\n";
        result_str = oss.str();
      }
    }

    // Clean up query is handled by unique_ptr

    // Only store result and increment count if it's not a WaitQuery
    if (!is_wait_query)
    {
      manager->output_pool.addResult(query_id, result_str);

      // Increment completed query count (only for real queries, not WaitQuery)
      manager->completed_query_count.fetch_add(1);
    }
  }
}
