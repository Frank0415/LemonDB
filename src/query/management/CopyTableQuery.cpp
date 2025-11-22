#include "CopyTableQuery.h"

#include <cstddef>
#include <exception>
#include <future>
#include <iterator>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../../db/Database.h"
#include "../../db/Table.h"
#include "../../db/TableLockManager.h"
#include "../../threading/Threadpool.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"

QueryResult::Ptr CopyTableQuery::execute() {
  try {
    auto &database = Database::getInstance();
    auto srcLock =
        TableLockManager::getInstance().acquireRead(this->targetTableRef());
    auto &src = database[this->targetTableRef()];

    // Validate source table
    auto validation_result = validateSourceTable(src);
    if (validation_result != nullptr) [[unlikely]] {
      return validation_result;
    }

    // Check if target table already exists
    auto dstLock =
        TableLockManager::getInstance().acquireWrite(this->newTableName);
    bool targetExists = false;
    try {
      (void)database[this->newTableName];
      targetExists = true;
    } catch (...) {
      // Intentionally ignore: table doesn't exist, which is expected
      (void)0;
    }
    if (targetExists) [[unlikely]] {
      wait_sem->release();
      return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                              "Target table name exists");
    }

    // Get field names
    const std::vector<std::string> fields = src.field();

    // Collect data from source table
    std::vector<RowData> collected_rows;
    if (!ThreadPool::isInitialized()) [[unlikely]] {
      collected_rows = collectSingleThreaded(src, fields);
    } else [[likely]] {
      const ThreadPool &pool = ThreadPool::getInstance();
      if (!is_multithreaded || pool.getThreadCount() <= 1 ||
          src.size() < Table::splitsize()) [[unlikely]] {
        collected_rows = collectSingleThreaded(src, fields);
      } else [[likely]] {
        collected_rows = collectMultiThreaded(src, fields);
      }
    }

    // Create target table and insert all collected rows
    auto dup = std::make_unique<Table>(this->newTableName, fields);

    for (auto &[key, row] : collected_rows) [[likely]] {
      dup->insertByIndex(key, std::move(row));
    }

    // Register the new table
    database.registerTable(std::move(dup));

    // Release the wait semaphore to allow queries on the new table to proceed
    wait_sem->release();

    return std::make_unique<SuccessMsgResult>(qname, this->targetTableRef());
  } catch (const TableNameNotFound &) {
    wait_sem->release();
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            "No such table.");
  } catch (const std::exception &exc) {
    wait_sem->release();
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            "Unknown error");
  }
}

std::string CopyTableQuery::toString() {
  return "QUERY = COPYTABLE, SOURCE = \"" + this->targetTableRef() +
         "\", TARGET = \"" + this->newTableName + "\"";
}

[[nodiscard]] QueryResult::Ptr
CopyTableQuery::validateSourceTable(const Table &src) const {
  if (src.empty()) [[unlikely]] {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            "Source table is empty.");
  }
  return nullptr;
}

[[nodiscard]] std::vector<CopyTableQuery::RowData>
CopyTableQuery::collectSingleThreaded(const Table &src,
                                      const std::vector<std::string> &fields) {
  std::vector<RowData> results;
  results.reserve(src.size());

  for (const auto &obj : src) [[likely]]
  {
    std::vector<Table::ValueType> row;
    row.reserve(fields.size());
    for (size_t field_idx = 0; field_idx < fields.size(); ++field_idx)
        [[likely]]
    {
      row.push_back(obj[field_idx]);
    }
    results.emplace_back(obj.key(), std::move(row));
  }

  return results;
}

[[nodiscard]] std::vector<CopyTableQuery::RowData>
CopyTableQuery::collectMultiThreaded(const Table &src,
                                     const std::vector<std::string> &fields) {
  constexpr size_t CHUNK_SIZE = Table::splitsize();
  const ThreadPool &pool = ThreadPool::getInstance();
  std::vector<RowData> results;

  // Create chunks and submit tasks
  std::vector<std::pair<size_t, std::future<std::vector<RowData>>>> tasks;
  tasks.reserve((src.size() + CHUNK_SIZE - 1) / CHUNK_SIZE);

  auto iterator = src.begin();
  size_t chunk_index = 0;
  while (iterator != src.end()) [[likely]] {
    auto chunk_begin = iterator;
    size_t count = 0;
    while (iterator != src.end() && count < CHUNK_SIZE) [[likely]] {
      ++iterator;
      ++count;
    }
    auto chunk_end = iterator;

    tasks.emplace_back(
        chunk_index, pool.submit([chunk_begin, chunk_end, &fields]() {
          std::vector<RowData> local_results;
          for (auto iter = chunk_begin; iter != chunk_end; ++iter) [[likely]]
          {
            std::vector<Table::ValueType> row;
            row.reserve(fields.size());
            for (size_t field_idx = 0; field_idx < fields.size(); ++field_idx)
                [[likely]]
            {
              row.push_back((*iter)[field_idx]);
            }
            local_results.emplace_back(iter->key(), std::move(row));
          }
          return local_results;
        }));
    ++chunk_index;
  }

  // Collect results in order by chunk_index
  for (auto &[idx, future] : tasks) [[likely]] {
    auto local_results = future.get();
    results.insert(results.end(),
                   std::make_move_iterator(local_results.begin()),
                   std::make_move_iterator(local_results.end()));
  }

  return results;
}
