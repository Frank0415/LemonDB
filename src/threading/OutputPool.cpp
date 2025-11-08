#include "OutputPool.h"

#include <cstddef>
#include <iostream>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

void OutputPool::addResult(size_t query_id, const std::string& result)
{
  // std::cerr << "Adding result for query_id " << query_id << "\n";
  const std::scoped_lock lock(results_mutex);
  results[query_id] = result;
}

size_t OutputPool::flushContinuousResults()
{
  std::vector<std::pair<size_t, std::string>> ready_results;
  {
    const std::scoped_lock lock(results_mutex);
    while (true)
    {
      const auto iter = results.find(next_output_id);
      if (iter == results.end())
      {
        break;
      }

      ready_results.emplace_back(iter->first, iter->second);
      results.erase(iter);
      ++next_output_id;
    }
  }

  const size_t flushed_count = ready_results.size();

  for (const auto& [query_id, result_string] : ready_results)
  {
    const bool is_quit = result_string.find("QUIT") != std::string::npos;

    if (!is_quit) [[unlikely]]
    {
      std::cout << query_id << "\n";
    }

    if (!result_string.empty())
    {
      std::cout << result_string;
    }
  }

  if (flushed_count > 0)
  {
    std::cout.flush();
  }

  total_output_count.fetch_add(flushed_count, std::memory_order_relaxed);
  return flushed_count;
}

void OutputPool::outputAllResults()
{
  while (flushContinuousResults() > 0)
  {
  }
}

size_t OutputPool::getResultCount() const
{
  const std::scoped_lock lock(results_mutex);
  return results.size();
}

size_t OutputPool::getNextOutputId() const
{
  const std::scoped_lock lock(results_mutex);
  return next_output_id;
}

size_t OutputPool::getTotalOutputCount() const
{
  return total_output_count.load(std::memory_order_relaxed);
}
