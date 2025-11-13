#include "MaxQuery.h"

#include <algorithm>
#include <cstddef>
#include <exception>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "db/Database.h"
#include "db/Table.h"
#include "db/TableLockManager.h"
#include "query/QueryResult.h"
#include "threading/Threadpool.h"
#include "utils/formatter.h"
#include "utils/uexception.h"

QueryResult::Ptr MaxQuery::execute() {
  try {
    if (validateOperands() != nullptr) [[unlikely]] {
      return validateOperands();
    }

    Database &database = Database::getInstance();
    auto lock =
        TableLockManager::getInstance().acquireRead(this->targetTableRef());
    auto &table = database[this->targetTableRef()];

    auto result = initCondition(table);

    if (!result.second) [[unlikely]] {
      return std::make_unique<NullQueryResult>();
    }

    auto fieldId = getFieldIndices(table);

    if (!ThreadPool::isInitialized()) [[unlikely]] {
      return executeSingleThreaded(table, fieldId);
    }

    const ThreadPool &pool = ThreadPool::getInstance();

    if (pool.getThreadCount() <= 1 || table.size() < Table::splitsize())
        [[unlikely]] {
      return executeSingleThreaded(table, fieldId);
    }
    return executeMultiThreaded(table, fieldId);

    return std::make_unique<NullQueryResult>();
  } catch (const TableNameNotFound &e) {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            std::string("No such table."));
  } catch (const TableFieldNotFound &e) {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            e.what());
  } catch (const IllFormedQueryCondition &e) {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            e.what());
  } catch (const std::invalid_argument &e) {
    // Cannot convert operand to string
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            "Unknown error '?'"_f % e.what());
  } catch (const std::exception &e) {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            "Unknown error '?'."_f % e.what());
  }
}

std::string MaxQuery::toString() {
  return "QUERY = MAX " + this->targetTableRef();
}

[[nodiscard]] QueryResult::Ptr MaxQuery::validateOperands() const {
  if (this->getOperands().empty()) [[unlikely]] {
    return std::make_unique<ErrorMsgResult>(
        qname, this->targetTableRef().c_str(),
        "No operand (? operands)."_f % getOperands().size());
  }
  return nullptr;
}

[[nodiscard]] std::vector<Table::FieldIndex>
MaxQuery::getFieldIndices(const Table &table) const {
  std::vector<Table::FieldIndex> fieldId;
  for (const auto &operand : this->getOperands()) [[likely]]
  {
    if (operand == "KEY") [[unlikely]] {
      throw IllFormedQueryCondition(
          "MAX operation not supported on KEY field.");
    }
    fieldId.push_back(table.getFieldIndex(operand));
  }
  return fieldId;
}

[[nodiscard]] QueryResult::Ptr
MaxQuery::executeSingleThreaded(Table &table,
                                const std::vector<Table::FieldIndex> &fids) {
  bool found = false;
  std::vector<Table::ValueType> maxValue(
      fids.size(),
      Table::ValueTypeMin);  // each has its own max value

  for (const auto &row : table) [[likely]]
  {
    if (this->evalCondition(row)) [[likely]] {
      found = true;

      for (size_t i = 0; i < fids.size(); ++i) [[likely]]
      {
        maxValue[i] = std::max(maxValue[i], row[fids[i]]);
      }
    }
  }

  if (!found) [[unlikely]] {
    return std::make_unique<NullQueryResult>();
  }
  return std::make_unique<SuccessMsgResult>(maxValue);
}

[[nodiscard]] QueryResult::Ptr
MaxQuery::executeMultiThreaded(Table &table,
                               const std::vector<Table::FieldIndex> &fids) {
  constexpr size_t CHUNK_SIZE = Table::splitsize();
  const ThreadPool &pool = ThreadPool::getInstance();
  const size_t num_fields = fids.size();
  std::vector<Table::ValueType> maxValues(num_fields, Table::ValueTypeMin);

  // Create chunks and submit tasks
  std::vector<std::future<std::vector<Table::ValueType>>> futures;
  auto iterator = table.begin();
  while (iterator != table.end()) [[likely]] {
    auto chunk_begin = iterator;
    size_t count = 0;
    while (iterator != table.end() && count < CHUNK_SIZE) [[likely]] {
      ++iterator;
      ++count;
    }
    auto chunk_end = iterator;

    futures.push_back(pool.submit([this, fids, chunk_begin, chunk_end,
                                   num_fields]() {
      std::vector<Table::ValueType> local_max(num_fields, Table::ValueTypeMin);
      for (auto it = chunk_begin; it != chunk_end; ++it) [[likely]]
      {
        if (this->evalCondition(*it)) [[likely]] {
          for (size_t i = 0; i < num_fields; ++i) [[likely]]
          {
            local_max[i] = std::max(local_max[i], (*it)[fids[i]]);
          }
        }
      }
      return local_max;
    }));
  }
  bool any_found = false;
  for (auto &future : futures) [[likely]]
  {
    auto local_max = future.get();
    for (size_t i = 0; i < num_fields; ++i) [[likely]]
    {
      if (!any_found && local_max[i] != Table::ValueTypeMin) [[unlikely]] {
        any_found = true;
      }
      maxValues[i] = std::max(maxValues[i], local_max[i]);
    }
  }
  if (!any_found) [[unlikely]] {
    return std::make_unique<NullQueryResult>();
  }
  return std::make_unique<SuccessMsgResult>(maxValues);
}
