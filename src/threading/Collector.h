#ifndef THREADING_COLLECTOR_H
#define THREADING_COLLECTOR_H

#include "db/Table.h"
#include <atomic>
#include <iostream>
#include <map>
#include <mutex>
#include <string>

class QueryResultCollector
{
private:
  std::map<size_t, std::string> results;
  mutable std::mutex results_mutex;
  std::atomic<size_t> expected_queries;
  std::atomic<size_t> completed_queries;

public:
  QueryResultCollector() : expected_queries(0), completed_queries(0)
  {
  }

  void setExpectedQueries(size_t count)
  {
    expected_queries.store(count);
  }

  void addResult(size_t query_id, const std::string& result)
  {
    {
      const std::scoped_lock<std::mutex> lock(results_mutex);
      results[query_id] = result;
    }
    const size_t new_completed = completed_queries.fetch_add(1) + 1;
    const size_t expected = expected_queries.load();
    std::cerr << "[Collector] Added result for query " << query_id
              << ". Completed: " << new_completed << "/" << expected << "\n";
  }

  bool allResultsCollected() const
  {
    const size_t completed = completed_queries.load();
    const size_t expected = expected_queries.load();
    return completed == expected && expected > 0;
  }

  void outputAllResults()
  {
    const std::scoped_lock<std::mutex> lock(results_mutex);

    const size_t completed = completed_queries.load();
    const size_t expected = expected_queries.load();

    if (results.empty())
    {
      std::cerr << "[Collector] Not outputting results: completed=" << completed
                << ", expected=" << expected << "\n";
      return;
    }

    std::cerr << "[Collector] Collected " << results.size() << " results. Query IDs:";
    for (const auto& [query_key, result_value] : results)
    {
      (void)result_value;
      std::cerr << ' ' << query_key;
    }
    std::cerr << "\n";

    auto const debug_iter = results.find(114);
    if (debug_iter != results.end())
    {
      std::cerr << "[Collector] Query 114 result length: " << debug_iter->second.size() << "\n";
      std::cerr << "[Collector] Query 114 result raw: '" << debug_iter->second << "'\n";
    }

    // Find the last query ID (which is QUIT)
    if (!results.empty())
    {
      const size_t last_query_id = results.rbegin()->first;
      for (const auto& [query_id, result] : results)
      {
        // Skip output for QUIT query (last query)
        if (query_id != last_query_id)
        {
          std::cout << query_id << "\n" << result;
        }
      }
    }
  }
};

void executeQueryAsync(Query::Ptr query, size_t query_id, QueryResultCollector& g_result_collector);
void executeQueryAsync(const std::shared_ptr<Query>& query,
                       size_t query_id,
                       QueryResultCollector& g_result_collector);

#endif // THREADING_COLLECTOR_H