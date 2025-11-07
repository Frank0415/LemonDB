#ifndef PROJECT_QUERY_MANAGER_H
#define PROJECT_QUERY_MANAGER_H

#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
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
 * - Each table has its own execution thread (runs queries serially for that table)
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
class QueryManager
{
private:
  // Single entry in query queue
  struct QueryEntry
  {
    size_t query_id;
    Query* query_ptr; // Raw pointer, owned by deque
  };

  // ============ Per-Table Execution ============
  // Map: table_name → queue of (query_id, query_ptr)
  std::unordered_map<std::string, std::deque<QueryEntry>> table_query_map;

  // Map: table_name → counting_semaphore (tracks available queries in queue)
  std::unordered_map<std::string, std::unique_ptr<std::counting_semaphore<>>> table_query_sem;


};

#endif // PROJECT_QUERY_MANAGER_H
