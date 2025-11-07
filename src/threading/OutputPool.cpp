#include "OutputPool.h"

#include <iostream>

void OutputPool::addResult(size_t query_id, const std::string& result)
{
  const std::scoped_lock lock(results_mutex);
  results[query_id] = result;
  // std::cerr << "[OutputPool] Added result for query " << query_id << " (total: " << result
  //           << ")\n";
}

void OutputPool::outputAllResults()
{
  const std::scoped_lock lock(results_mutex);
  // std::cerr << "[OutputPool] Outputting " << results.size() << " results\n";
  for (const auto& [query_id, result_string] : results)
  {
    // Don't print query number for QUIT queries
    const bool is_quit = result_string.find("QUIT") != std::string::npos;

    if (!is_quit) [[unlikely]]
    {
      std::cout << query_id << "\n";
    }

    if (!result_string.empty())
    {
      std::cout << result_string;
    }

    std::cout.flush();

    // std::cerr << "[OutputPool] Printed result for query " << query_id
    //           << (is_quit ? " (QUIT - no number)" : "") << "\n";
  }

  // std::cerr << "[OutputPool] All results output complete\n";
}

size_t OutputPool::getResultCount() const
{
  const std::scoped_lock lock(results_mutex);
  return results.size();
}
