#ifndef THREADING_COLLECTOR_H
#define THREADING_COLLECTOR_H

#include <iostream>
#include <map>
#include <mutex>
#include <string>

struct QueryResultCollector {
  std::map<size_t, std::string> results;
  std::mutex results_mutex;

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

#endif // THREADING_COLLECTOR_H