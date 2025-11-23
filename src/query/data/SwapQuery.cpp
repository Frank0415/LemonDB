#include "SwapQuery.h"

#include <cstddef>
#include <exception>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "../../db/Database.h"
#include "../../db/TableLockManager.h"
#include "../../utils/formatter.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"
#include "./db/Table.h"
#include "./threading/Threadpool.h"

QueryResult::Ptr SwapQuery::execute() {
  try {
    auto validation_result = validateOperands();
    if (validation_result != nullptr) [[unlikely]] {
      return validation_result;
    }

    auto lock =
        TableLockManager::getInstance().acquireWrite(this->targetTableRef());
    Database &database = Database::getInstance();
    auto &table = database[this->targetTableRef()];

    auto result = initCondition(table);
    if (!result.second) [[unlikely]] {
      return std::make_unique<RecordCountResult>(0);
    }

    const auto fids = getFieldIndices(table);
    auto field_index_1 = fids.first;
    auto field_index_2 = fids.second;

    // Try KEY condition optimization first
    Table::SizeType counter = 0;
    const bool handled = this->testKeyCondition(
        table, [&](bool success, Table::Object::Ptr obj) {
          if (!success) [[unlikely]] {
            return;
          }
          if (obj) [[likely]] {
            auto tmp = (*obj)[field_index_1];
            (*obj)[field_index_1] = (*obj)[field_index_2];
            (*obj)[field_index_2] = tmp;
            ++counter;
          }
        });

    if (handled) [[unlikely]] {
      return std::make_unique<RecordCountResult>(static_cast<int>(counter));
    }

    // Use ThreadPool if available
    if (!ThreadPool::isInitialized()) [[unlikely]] {
      return executeSingleThreaded(table, field_index_1, field_index_2);
    }

    const ThreadPool &pool = ThreadPool::getInstance();
    if (pool.getThreadCount() <= 1 || table.size() < Table::splitsize())
        [[unlikely]] {
      return executeSingleThreaded(table, field_index_1, field_index_2);
    }

    return executeMultiThreaded(table, field_index_1, field_index_2);
  } catch (const TableNameNotFound &) {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            "No such table.");
  } catch (const IllFormedQueryCondition &e) {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            e.what());
  } catch (const std::invalid_argument &e) {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            "Unknown error '?'"_f % e.what());
  } catch (const std::exception &e) {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            "Unkonwn error '?'."_f % e.what());
  }
}
std::string SwapQuery::toString() {
  return "QUERY = SWAP \"" + this->targetTableRef() + "\"";
}

[[nodiscard]] QueryResult::Ptr SwapQuery::validateOperands() const {
  if (this->getOperands().size() != 2) [[unlikely]] {
    return std::make_unique<ErrorMsgResult>(
        qname, this->targetTableRef().c_str(),
        "Invalid number of operands (? operands)."_f % getOperands().size());
  }
  return nullptr;
}

[[nodiscard]] std::pair<const Table::FieldIndex, const Table::FieldIndex>
SwapQuery::getFieldIndices(const Table &table) const {
  if (getOperands()[0] == "KEY" || getOperands()[1] == "KEY") [[unlikely]] {
    throw std::make_unique<ErrorMsgResult>(
        qname, this->targetTableRef(),
        "Ill-formed query: KEY cannot be swapped.");
  }
  return {table.getFieldIndex(getOperands()[0]),
          table.getFieldIndex(getOperands()[1])};
}

// cppcheck-suppress constParameter
[[nodiscard]] QueryResult::Ptr
SwapQuery::executeSingleThreaded(Table &table,
                                 const Table::FieldIndex &field_index_1,
                                 const Table::FieldIndex &field_index_2) {
  Table::SizeType counter = 0;

  for (auto row : table) [[likely]]
  {
    if (this->evalCondition(row)) [[likely]] {
      auto tmp = row[field_index_1];
      row[field_index_1] = row[field_index_2];
      row[field_index_2] = tmp;
      ++counter;
    }
  }
  return std::make_unique<RecordCountResult>(static_cast<int>(counter));
}

// cppcheck-suppress constParameter
[[nodiscard]] QueryResult::Ptr
SwapQuery::executeMultiThreaded(Table &table,
                                const Table::FieldIndex &field_index_1,
                                const Table::FieldIndex &field_index_2) {
  constexpr size_t CHUNK_SIZE = Table::splitsize();
  const ThreadPool &pool = ThreadPool::getInstance();
  std::vector<std::future<Table::SizeType>> futures;
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

    futures.push_back(pool.submit(
        [this, chunk_begin, chunk_end, field_index_1, field_index_2]() {
          Table::SizeType local_counter = 0;
          for (auto it = chunk_begin; it != chunk_end; ++it) [[likely]]
          {
            if (this->evalCondition(*it)) [[likely]] {
              auto tmp = (*it)[field_index_1];
              (*it)[field_index_1] = (*it)[field_index_2];
              (*it)[field_index_2] = tmp;
              ++local_counter;
            }
          }
          return local_counter;
        }));
  }
  Table::SizeType counter = 0;
  for (auto &future : futures) [[likely]]
  {
    counter += future.get();
  }
  return std::make_unique<RecordCountResult>(static_cast<int>(counter));
}