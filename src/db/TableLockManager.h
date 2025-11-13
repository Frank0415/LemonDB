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

  [[nodiscard]] std::shared_lock<std::shared_mutex>
  acquireRead(const std::string &table_name) {
    return std::shared_lock<std::shared_mutex>(getOrCreateLock(table_name));
  }

  [[nodiscard]] std::unique_lock<std::shared_mutex>
  acquireWrite(const std::string &table_name) {
    return std::unique_lock<std::shared_mutex>(getOrCreateLock(table_name));
  }

  [[nodiscard]] static TableLockManager &getInstance() {
    static TableLockManager instance;
    return instance;
  }
};

#endif  // PROJECT_TABLELOCKMANAGER_H