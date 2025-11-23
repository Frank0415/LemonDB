#ifndef PROJECT_TABLELOCKMANAGER_H
#define PROJECT_TABLELOCKMANAGER_H

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>

/**
 * Thread-safe per-table lock manager providing shared (read) and exclusive
 * (write) locking primitives using `std::shared_mutex`.
 *
 * Each table name is associated lazily with a `std::shared_mutex` created on
 * first access. Lookups use a two-phase strategy: an initial shared lock to
 * check existence followed by an exclusive lock only when insertion is needed.
 *
 * Usage:
 *  - Obtain a lock object via `acquireRead(table)` or `acquireWrite(table)`.
 *  - The returned lock objects manage locking state via RAII and should be
 *    held in local scope; releasing them unlocks the associated table mutex.
 *  - This class is a singleton; access via `getInstance()`.
 *
 * Notes:
 *  - Lock objects are returned by value; they encapsulate the acquired lock.
 *  - Multiple readers may hold shared locks concurrently; writers are mutually
 *    exclusive and block until all readers release their locks.
 *  - No attempt is made to garbage-collect unused mutexes; they persist for
 *    the lifetime of the process once created.
 */
class TableLockManager {
private:
  /**
   * Map from table name to its associated shared mutex.
   */
  std::unordered_map<std::string, std::unique_ptr<std::shared_mutex>> lock_map_;
  /**
   * Mutex protecting access to the lock map (supports reader concurrency).
   */
  mutable std::shared_mutex map_mutex_;

  TableLockManager() = default;
  ~TableLockManager() = default;

  /**
   * Retrieve the shared mutex for a table, creating it if absent.
   * Performs an initial shared (read) lookup, then upgrades with an exclusive
   * lock only on miss to minimize contention.
   * @param table_name Name of the table whose lock is requested.
   * @return Reference to the table's shared mutex.
   */
  std::shared_mutex &getOrCreateLock(const std::string &table_name) {
    {
      const std::shared_lock<std::shared_mutex> read(map_mutex_);
      auto iter = lock_map_.find(table_name);
      if (iter != lock_map_.end()) [[likely]] {
        return *iter->second;
      }
    }

    const std::unique_lock<std::shared_mutex> write(map_mutex_);
    auto &ptr = lock_map_[table_name];
    if (!ptr) [[unlikely]] {
      ptr = std::make_unique<std::shared_mutex>();
    }
    return *ptr;
  }

public:
  TableLockManager(TableLockManager &&) = delete;
  TableLockManager &operator=(TableLockManager &&) = delete;
  TableLockManager(const TableLockManager &) = delete;
  TableLockManager &operator=(const TableLockManager &) = delete;

  /**
   * Acquire a shared (read) lock for the specified table.
   * Multiple concurrent read locks are permitted.
   * @param table_name Table name to lock for read.
   * @return `std::shared_lock` owning the acquired shared mutex state.
   */
  [[nodiscard]] std::shared_lock<std::shared_mutex>
  acquireRead(const std::string &table_name) {
    return std::shared_lock<std::shared_mutex>(getOrCreateLock(table_name));
  }

  /**
   * Acquire an exclusive (write) lock for the specified table.
   * Blocks until all readers and writers release the mutex.
   * @param table_name Table name to lock for write.
   * @return `std::unique_lock` owning the acquired exclusive mutex state.
   */
  [[nodiscard]] std::unique_lock<std::shared_mutex>
  acquireWrite(const std::string &table_name) {
    return std::unique_lock<std::shared_mutex>(getOrCreateLock(table_name));
  }

  /**
   * Get the singleton instance of the lock manager.
   * @return Reference to the global `TableLockManager` instance.
   */
  [[nodiscard]] static TableLockManager &getInstance() {
    static TableLockManager instance;
    return instance;
  }
};

#endif  // PROJECT_TABLELOCKMANAGER_H