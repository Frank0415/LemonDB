#ifndef THREADING_COLLECTOR_H
#define THREADING_COLLECTOR_H

#include "query/Query.h"
#include <iostream>
#include <map>
#include <mutex>
#include <string>

class QueryResultCollector
{
private:
  std::map<size_t, std::string> results;
  std::mutex results_mutex;

public:
  void addResult(size_t query_id, const std::string& result)
  {
    std::lock_guard<std::mutex> lock(results_mutex);
    results[query_id] = result;
  }

  void outputAllResults()
  {
    if (results.empty())
    {
      return;
    }

    // Find the last query ID (which is QUIT)
    size_t last_query_id = results.rbegin()->first;

    for (const auto& [query_id, result] : results)
    {
      // Skip output for QUIT query (last query)
      if (query_id != last_query_id)
      {
        std::cout << query_id << "\n" << result;
      }
    }
  }
};

void executeQueryAsync(Query::Ptr query, size_t query_id, QueryResultCollector& g_result_collector);

#endif // THREADING_COLLECTOR_H