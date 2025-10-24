#ifndef PROJECT_TABLELOCKMANAGER_H
#define PROJECT_TABLELOCKMANAGER_H

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>

class TableLockManager {
private:
  std::unordered_map<std::string, std::unique_ptr<std::shared_mutex>> lock_map_;
  mutable std::shared_mutex map_mutex_;

  TableLockManager() = default;
  ~TableLockManager() = default;

  TableLockManager(const TableLockManager &) = delete;
  TableLockManager &operator=(const TableLockManager &) = delete;

  std::shared_mutex &getOrCreateLock(const std::string &table_name) {}

public:
};

#endif // PROJECT_TABLELOCKMANAGER_H