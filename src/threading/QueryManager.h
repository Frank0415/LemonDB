#ifndef PROJECT_QUERY_MANAGER_H
#define PROJECT_QUERY_MANAGER_H

#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <semaphore>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

class Query;
class QueryResult;
class OutputPool;

/**
 * QueryManager: Coordinates table-level parallel query execution
 *
 * Architecture:
 * - Main thread reads queries and submits them to per-table queues
 * - Each table has its own execution thread (runs queries serially for that
 * table)
 * - Multiple tables execute in parallel
 * - Results are collected in OutputPool (thread-safe map)
 *
 * Key Features:
 * Per-table queues: Queries for same table execute serially (no races)
 * Table-level parallelism: Different tables execute simultaneously
 * Result ordering: OutputPool maintains ordered map by query_id
 * Async submission: Main thread doesn't block on query execution
 * No print thread: OutputPool outputs all results at the end
 */
class QueryManager {
private:
  // Single entry in query queue
  struct QueryEntry {
    size_t query_id;
    Query *query_ptr;  // Raw pointer, owned by deque
  };

  // Map: table_name -> queue of (query_id, query_ptr)
  std::unordered_map<std::string, std::deque<QueryEntry>> table_query_map;

  // Map: table_name -> counting_semaphore (tracks available queries in queue)
  std::unordered_map<std::string, std::unique_ptr<std::counting_semaphore<>>>
      table_query_sem;

  // Protects table_query_map and table_query_sem
  mutable std::mutex table_map_mutex;

  std::vector<std::thread> table_threads;

  std::atomic<bool> is_end{false};
  std::atomic<bool> read_end{false};
  std::atomic<size_t> query_counter{0};
  std::atomic<size_t> expected_query_count{0};
  std::atomic<size_t> completed_query_count{0};

  // Reference to OutputPool (passed in constructor, not owned)
  OutputPool &output_pool;
  size_t printed_count{0};

  void createTableStructures(const std::string &table_name);
  void releaseSemaphores();
  void joinThreads();
  std::counting_semaphore<> *
  getSemaphoreForTable(const std::string &table_name);
  std::optional<QueryEntry> dequeueQuery(const std::string &table_name);
  void executeAndStoreResult(const QueryEntry &query_entry);

  /**
   * Execute queries for a specific table
   * Runs in a per-table thread
   */
  static void executeQueryForTable(QueryManager *manager,
                                   const std::string &table_name);

  /**
   * Print results in order
   * Runs in a dedicated print thread
   */
  static void printResults(QueryManager *manager);

public:
  QueryManager() = delete;

  // Explicit constructor taking OutputPool reference
  /**
   * Construct QueryManager with reference to OutputPool
   * @param pool Reference to the OutputPool for result collection
   */
  explicit QueryManager(OutputPool &pool) : output_pool(pool) {}

  // Deleted copy/move operations
  QueryManager(const QueryManager &) = delete;
  QueryManager &operator=(const QueryManager &) = delete;
  QueryManager(QueryManager &&) = delete;
  QueryManager &operator=(QueryManager &&) = delete;

  ~QueryManager();

  /**
   * Submit a query to the appropriate table queue
   * Creates table'st execution thread if needed
   * Does NOT block - returns immediately
   * @param query_id Unique identifier for the query
   * @param table_name Name of the table this query operates on
   * @param query_ptr Pointer to the query object to execute
   */
  void addQuery(size_t query_id, const std::string &table_name,
                Query *query_ptr);

  /**
   * Immediately publish a query result without scheduling execution.
   * Used for instant queries executed on the caller thread (e.g., LISTEN).
   */
  void addImmediateResult(size_t query_id, const std::string &result);

  /**
   * Set the expected number of queries (to know when all are done)
   * @param count The total number of queries expected to be executed
   */
  void setExpectedQueryCount(size_t count);

  /**
   * Wait for all queries to be executed
   * Blocks main thread until all table threads complete
   */
  void waitForCompletion();

  /**
   * Shutdown and cleanup
   */
  void shutdown();

  /**
   * Check if all expected queries have been completed
   */
  [[nodiscard]] bool isComplete() const;

  /**
   * Get the number of queries that have been completed
   */
  [[nodiscard]] size_t getCompletedQueryCount() const;

  /**
   * Get the expected number of queries to be executed
   */
  [[nodiscard]] size_t getExpectedQueryCount() const;
};

#endif  // PROJECT_QUERY_MANAGER_H
