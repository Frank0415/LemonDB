#ifndef THREADING_COLLECTOR_H
#define THREADING_COLLECTOR_H

#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include "query/Query.h"

class QueryResultCollector {
private:
  std::map<size_t, std::string> results;
  std::mutex results_mutex;

public:
  void addResult(size_t query_id, const std::string &result) {
    std::lock_guard<std::mutex> lock(results_mutex);
    results[query_id] = result;
  }

  void outputAllResults() {
    for (const auto &[query_id, result] : results) {
      std::cout << query_id << "\n" << result;
    }
    std::cout << std::flush;
  }
};

void executeQueryAsync(Query::Ptr query, size_t query_id, QueryResultCollector &g_result_collector) {
  try {
    QueryResult::Ptr result = query->execute();

    std::ostringstream oss;
    if (result) {
      oss << *result;
    }

    g_result_collector.addResult(query_id, oss.str());

  } catch (const std::exception &e) {
    std::ostringstream oss;
    oss << "Error: " << e.what() << "\n";
    g_result_collector.addResult(query_id, oss.str());
  }
}

#endif // THREADING_COLLECTOR_H