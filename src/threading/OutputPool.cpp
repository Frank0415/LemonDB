#include "OutputPool.h"

#include <cstddef>
#include <iostream>
#include <mutex>
#include <string>

void OutputPool::addResult(size_t query_id, const std::string& result)
{
  const std::scoped_lock lock(results_mutex);
  results[query_id] = result;
}

void OutputPool::outputAllResults()
{
  const std::scoped_lock lock(results_mutex);
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
  }
}

size_t OutputPool::getResultCount() const
{
  const std::scoped_lock lock(results_mutex);
  return results.size();
}
