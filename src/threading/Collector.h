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
