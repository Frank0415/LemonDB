#ifndef PROJECT_OUTPUT_POOL_H
#define PROJECT_OUTPUT_POOL_H

#include <map>
#include <mutex>
#include <string>

/**
 * OutputPool: Thread-safe result buffering and ordering
 *
 * NOT a singleton - passed by reference to avoid global state
 * - Each thread adds results as they complete: addResult(query_id, result_string)
 * - Results are stored in an ordered map (by query_id)
 * - At the end, call outputAllResults() to print everything in order
 *
 * This eliminates the need for a dedicated print thread and semaphore polling.
 */
class OutputPool
{
private:
  // Ordered map: query_id -> result_string
  std::map<size_t, std::string> results;
  mutable std::mutex results_mutex;

public:
  OutputPool() = default;

  // Default copy/move operations allowed
  OutputPool(const OutputPool&) = delete;
  OutputPool& operator=(const OutputPool&) = delete;
  OutputPool(OutputPool&&) = delete;
  OutputPool& operator=(OutputPool&&) = delete;

  ~OutputPool() = default;

  /**
   * Add a result to the output pool
   * Thread-safe - can be called from multiple threads simultaneously
   */
  void addResult(size_t query_id, const std::string& result);

  /**
   * Output all results in order (query_id ascending)
   * Should be called after all queries have finished
   * Does NOT print query numbers for QUIT queries
   */
  void outputAllResults();

  /**
   * Get number of results collected
   */
  [[nodiscard]] size_t getResultCount() const;
};

#endif  // PROJECT_OUTPUT_POOL_H
