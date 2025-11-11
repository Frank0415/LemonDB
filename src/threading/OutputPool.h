#ifndef PROJECT_OUTPUT_POOL_H
#define PROJECT_OUTPUT_POOL_H

#include <atomic>
#include <map>
#include <mutex>
#include <string>

/**
 * OutputPool: Thread-safe result buffering and ordering
 *
 * - Each thread adds results as they complete: addResult(query_id, result_string)
 * - Results are stored in an ordered map (by query_id)
 * - At the end, call outputAllResults() to print everything in order
 */
class OutputPool
{
private:
  // Ordered map: query_id -> result_string
  std::map<size_t, std::string> results;
  size_t next_output_id = 1;
  mutable std::mutex results_mutex;
  std::atomic<size_t> total_output_count{0};

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
   * @param query_id The unique identifier for the query result
   * @param result The result string to add to the pool
   */
  void addResult(size_t query_id, std::string& result);

  /**
   * Flush ready results in order (streaming)
   * Prints and frees continuous results starting from next_output_id
   * Returns the number of results flushed this call
   */
  size_t flushContinuousResults();

  /**
   * Output all currently buffered results in order.
   * Provided for compatibility with call sites expecting a bulk flush.
   */
  void outputAllResults();

  /**
   * Get number of results collected
   */
  [[nodiscard]] size_t getResultCount() const;

  /**
   * Returns the next query id expected to be printed.
   */
  [[nodiscard]] size_t getNextOutputId() const;

  /**
   * Returns the total number of results that have been flushed.
   */
  [[nodiscard]] size_t getTotalOutputCount() const;
};

#endif // PROJECT_OUTPUT_POOL_H
