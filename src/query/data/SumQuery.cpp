#include "SumQuery.h"

#include <algorithm>
#include <cstddef>
#include <exception>
#include <future>
#include <memory>
#include <string>
#include <vector>

#include "../../db/Database.h"
#include "../../db/Table.h"
#include "../../db/TableLockManager.h"
#include "../../threading/Threadpool.h"
#include "../../utils/formatter.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"

[[nodiscard]] QueryResult::Ptr SumQuery::execute() {
  Database &database = Database::getInstance();

  try {
    // Validate operands
    auto validation_result = validateOperands();
    if (validation_result != nullptr) [[unlikely]] {
      return validation_result;
    }

    auto lock =
        TableLockManager::getInstance().acquireRead(this->targetTableRef());
    auto &table = database[this->targetTableRef()];

    auto result = initCondition(table);
    if (!result.second) [[unlikely]] {
      const size_t num_fields = getFieldIndices(table).size();
      const std::vector<Table::ValueType> sums(num_fields, 0);
      return std::make_unique<SuccessMsgResult>(sums);
    }

    // Get field indices
    auto fids = getFieldIndices(table);

    // Check if ThreadPool is available and has multiple threads
    if (!ThreadPool::isInitialized()) [[unlikely]] {
      return executeSingleThreaded(table, fids);
    }

    const ThreadPool &pool = ThreadPool::getInstance();
    if (pool.getThreadCount() <= 1 || table.size() < Table::splitsize())
        [[unlikely]] {
      return executeSingleThreaded(table, fids);
    }

    // Multi-threaded execution
    return executeMultiThreaded(table, fids);
  } catch (const TableNameNotFound &) {
    return std::make_unique<ErrorMsgResult>("SUM", this->targetTableRef(),
                                            "No such table.");
  } catch (const TableFieldNotFound &) {
    return std::make_unique<ErrorMsgResult>("SUM", this->targetTableRef(),
                                            "No such field.");
  } catch (const IllFormedQueryCondition &exc) {
    return std::make_unique<ErrorMsgResult>("SUM", this->targetTableRef(),
                                            exc.what());
  } catch (const std::exception &exc) {
    return std::make_unique<ErrorMsgResult>("SUM", this->targetTableRef(),
                                            "Unknown error '?'"_f % exc.what());
  }
}

[[nodiscard]] std::string SumQuery::toString() {
  return "QUERY = SUM \"" + this->targetTableRef() + "\"";
}

[[nodiscard]] QueryResult::Ptr SumQuery::validateOperands() const {
  if (this->getOperands().empty()) [[unlikely]] {
    return std::make_unique<ErrorMsgResult>("SUM", this->targetTableRef(),
                                            "Invalid number of fields");
  }
  if (std::any_of(this->getOperands().begin(), this->getOperands().end(),
                  [](const auto &field) { return field == "KEY"; }))
      [[unlikely]]
  {
    return std::make_unique<ErrorMsgResult>("SUM", this->targetTableRef(),
                                            "KEY cannot be summed.");
  }
  return nullptr;
}

[[nodiscard]] std::vector<Table::FieldIndex>
SumQuery::getFieldIndices(const Table &table) const {
  std::vector<Table::FieldIndex> fids;
  fids.reserve(this->getOperands().size());
  for (const auto &field : this->getOperands()) [[likely]]
  {
    fids.emplace_back(table.getFieldIndex(field));
  }
  return fids;
}

[[nodiscard]] QueryResult::Ptr
SumQuery::executeSingleThreaded(Table &table,
                                const std::vector<Table::FieldIndex> &fids) {
  const size_t num_fields = fids.size();
  std::vector<Table::ValueType> sums(num_fields, 0);

  for (auto row : table) [[likely]]
  {
    if (this->evalCondition(row)) [[likely]] {
      for (size_t idx = 0; idx < num_fields; ++idx) [[likely]]
      {
        sums[idx] += row[fids[idx]];
      }
    }
  }
  return std::make_unique<SuccessMsgResult>(sums);
}

[[nodiscard]] QueryResult::Ptr
SumQuery::executeMultiThreaded(Table &table,
                               const std::vector<Table::FieldIndex> &fids) {
  constexpr size_t CHUNK_SIZE = Table::splitsize();
  const ThreadPool &pool = ThreadPool::getInstance();
  const size_t num_fields = fids.size();
  std::vector<Table::ValueType> sums(num_fields, 0);

  // Create chunks and submit tasks
  std::vector<std::future<std::vector<Table::ValueType>>> futures;
  futures.reserve((table.size() + CHUNK_SIZE - 1) / CHUNK_SIZE);

  auto iterator = table.begin();
  while (iterator != table.end()) [[likely]] {
    auto chunk_begin = iterator;
    size_t count = 0;
    while (iterator != table.end() && count < CHUNK_SIZE) [[likely]] {
      ++iterator;
      ++count;
    }
    auto chunk_end = iterator;

    futures.push_back(
        pool.submit([this, fids, chunk_begin, chunk_end, num_fields]() {
          std::vector<Table::ValueType> local_sums(num_fields, 0);
          for (auto iter = chunk_begin; iter != chunk_end; ++iter) [[likely]]
          {
            if (this->evalCondition(*iter)) [[likely]] {
              for (size_t i = 0; i < num_fields; ++i) [[likely]]
              {
                local_sums[i] += (*iter)[fids[i]];
              }
            }
          }
          return local_sums;
        }));
  }

  // Combine results from all threads
  for (auto &future : futures) [[likely]]
  {
    auto local_sums = future.get();
    for (size_t i = 0; i < num_fields; ++i) [[likely]]
    {
      sums[i] += local_sums[i];
    }
  }

  return std::make_unique<SuccessMsgResult>(sums);
}
