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
