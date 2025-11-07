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
  // Must hold lock while accessing semaphore
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

  constexpr int poll_interval_ms = 5;

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

  // Release all semaphores to wake up threads
  {
    std::lock_guard lock(table_map_mutex);
    for (auto& sem_pair : table_query_sem)
    {
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

      // std::cerr << "[QueryManager] Query " << query_id << " executed successfully\n";
    }
    catch (const std::exception& exc)
    {
      // Check if this is a WaitQuery completion exception
      if (std::string(exc.what()).find("WaitQuery completed") != std::string::npos)
      {
        // WaitQuery completed - don't add to OutputPool
        // std::cerr << "[QueryManager] WaitQuery completed for table '" << table_name << "'\n";
        is_wait_query = true;
      }
      else
      {
        std::ostringstream oss;
        oss << "Error: " << exc.what() << "\n";
        result_str = oss.str();

        // std::cerr << "[QueryManager] Query " << query_id << " failed: " << exc.what() << "\n";
      }
    }

    // Clean up query
    delete query_ptr;

    // Only store result and increment count if it's not a WaitQuery
    if (!is_wait_query)
    {
      manager->output_pool.addResult(query_id, result_str);
      // std::cerr << "[QueryManager] Result queued for output (query " << query_id << ")\n";

      // Increment completed query count (only for real queries, not WaitQuery)
      manager->completed_query_count.fetch_add(1);
      // std::cerr << "[QueryManager] Completed queries: " << manager->completed_query_count.load()
      //           << "\n";
    }
    else
    {
      // std::cerr << "[QueryManager] WaitQuery result NOT queued for output (query " << query_id
      //           << ")\n";
      // std::cerr << "[QueryManager] WaitQuery does NOT increment completed count\n";
    }
  }

  // std::cerr << "[QueryManager] Table '" << table_name << "' thread exiting normally\n";
}
