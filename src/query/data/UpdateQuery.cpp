//
// Created by liu on 18-10-25.
//

#include "UpdateQuery.h"

#include <cstdlib>
#include <exception>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "../../db/Database.h"
#include "../../db/TableLockManager.h"
#include "../../threading/Threadpool.h"
#include "../../utils/formatter.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"

QueryResult::Ptr UpdateQuery::execute() {
  try {
    auto validationResult = validateOperands();
    if (validationResult != nullptr) [[unlikely]] {
      return validationResult;
    }

    Database &database = Database::getInstance();
    auto lock =
        TableLockManager::getInstance().acquireWrite(this->targetTableRef());
    auto &table = database[this->targetTableRef()];

    if (this->getOperands()[0] == "KEY") [[unlikely]] {
      this->keyValue = this->getOperands()[1];
    } else [[likely]] {
      constexpr int decimal_base = 10;
      this->fieldId = table.getFieldIndex(this->getOperands()[0]);
      this->fieldValue = static_cast<Table::ValueType>(
          strtol(this->getOperands()[1].c_str(), nullptr, decimal_base));
    }

    auto result = initCondition(table);
    if (!result.second) [[unlikely]] {
      return std::make_unique<RecordCountResult>(0);
    }

    if (!ThreadPool::isInitialized()) [[unlikely]] {
      return executeSingleThreaded(table);
    }

    const ThreadPool &pool = ThreadPool::getInstance();
    if (pool.getThreadCount() <= 1 || table.size() < Table::splitsize())
        [[unlikely]] {
      return executeSingleThreaded(table);
    }

    return executeMultiThreaded(table);
  } catch (const TableNameNotFound &e) {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            std::string("No such table."));
  } catch (const IllFormedQueryCondition &e) {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            e.what());
  } catch (const std::invalid_argument &e) {
    // Cannot convert operand to string
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            "Unknown error '?'"_f % e.what());
  } catch (const std::exception &e) {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            "Unkonwn error '?'."_f % e.what());
  }
}

std::string UpdateQuery::toString() {
  return "QUERY = UPDATE " + this->targetTableRef();
}

[[nodiscard]] QueryResult::Ptr UpdateQuery::validateOperands() const {
  if (this->getOperands().size() != 2) [[unlikely]] {
    return std::make_unique<ErrorMsgResult>(
        qname, this->targetTableRef().c_str(),
        "Invalid number of operands (? operands)."_f % getOperands().size());
  }
  return nullptr;
}

[[nodiscard]] QueryResult::Ptr
UpdateQuery::executeSingleThreaded(Table &table) {
  Table::SizeType counter = 0;
  for (auto it = table.begin(); it != table.end(); ++it) [[likely]]
  {
    if (this->evalCondition(*it)) [[likely]] {
      if (this->keyValue.empty()) [[likely]] {
        (*it)[this->fieldId] = this->fieldValue;
      } else [[unlikely]] {
        it->setKey(this->keyValue);
      }
      ++counter;
    }
  }
  return std::make_unique<RecordCountResult>(counter);
}

[[nodiscard]] QueryResult::Ptr UpdateQuery::executeMultiThreaded(Table &table) {
  constexpr size_t CHUNK_SIZE = Table::splitsize();
  const ThreadPool &pool = ThreadPool::getInstance();
  std::vector<std::future<Table::SizeType>> futures;
  futures.reserve((table.size() + CHUNK_SIZE - 1) / CHUNK_SIZE);

  auto iterator = table.begin();
  while (iterator != table.end()) [[likely]] {
    auto chunk_start = iterator;
    size_t count = 0;
    while (iterator != table.end() && count < CHUNK_SIZE) [[likely]] {
      ++iterator;
      ++count;
    }
    auto chunk_end = iterator;

    futures.push_back(pool.submit([this, chunk_start, chunk_end]() {
      Table::SizeType local_count = 0;
      for (auto it = chunk_start; it != chunk_end; ++it) [[likely]]
      {
        if (this->evalCondition(*it)) [[likely]] {
          if (this->keyValue.empty()) [[likely]] {
            (*it)[this->fieldId] = this->fieldValue;
          } else [[unlikely]] {
            it->setKey(this->keyValue);
          }
          ++local_count;
        }
      }
      return local_count;
    }));
  }

  // Wait for all tasks to complete and aggregate results
  Table::SizeType total_count = 0;
  for (auto &future : futures) [[likely]]
  {
    total_count += future.get();
  }
  return std::make_unique<RecordCountResult>(total_count);
}
