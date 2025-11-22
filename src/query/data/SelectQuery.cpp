#include "SelectQuery.h"

#include <algorithm>
#include <cstddef>
#include <exception>
#include <future>
#include <iterator>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "../../db/Database.h"
#include "../../db/Table.h"
#include "../../db/TableLockManager.h"
#include "../../threading/Threadpool.h"
#include "../../utils/formatter.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"

QueryResult::Ptr SelectQuery::execute() {
  try {
    auto validation_result = validateOperands();
    if (validation_result != nullptr) [[unlikely]] {
      return validation_result;
    }

    auto &database = Database::getInstance();
    auto lock =
        TableLockManager::getInstance().acquireRead(this->targetTableRef());
    auto &table = database[this->targetTableRef()];

    auto fieldIds = getFieldIndices(table);
    auto result = initCondition(table);

    if (!result.second) [[unlikely]] {
      return std::make_unique<TextRowsResult>("");
    }

    // Try KEY condition optimization first
    std::ostringstream key_buffer;
    const bool handled = this->testKeyCondition(
        table, [&](bool success, Table::Object::Ptr obj) {
          if (!success) [[unlikely]] {
            return;
          }
          if (obj) [[likely]] {
            key_buffer << "( " << obj->key();
            for (const auto &field_id : fieldIds) {
              key_buffer << " " << (*obj)[field_id];
            }
            key_buffer << " )\n";
          }
        });

    if (handled) [[unlikely]] {
      return std::make_unique<TextRowsResult>(key_buffer.str());
    }

    // Use ThreadPool if available
    if (!ThreadPool::isInitialized()) [[unlikely]] {
      return executeSingleThreaded(table, fieldIds);
    }

    const ThreadPool &pool = ThreadPool::getInstance();
    if (pool.getThreadCount() <= 1 || table.size() < Table::splitsize())
        [[unlikely]] {
      return executeSingleThreaded(table, fieldIds);
    }

    return executeMultiThreaded(table, fieldIds);
  } catch (const TableNameNotFound &) {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            "No such table.");
  } catch (const IllFormedQueryCondition &e) {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            e.what());
  } catch (const std::exception &e) {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            "Unknown error '?'."_f % e.what());
  }
}

std::string SelectQuery::toString() {
  return "QUERY = SELECT \"" + this->targetTableRef() + "\"";
}

[[nodiscard]] QueryResult::Ptr SelectQuery::validateOperands() const {
  if (this->getOperands().empty()) [[unlikely]] {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            "Invalid operands.");
  }
  return nullptr;
}

[[nodiscard]] std::vector<Table::FieldIndex>
SelectQuery::getFieldIndices(const Table &table) const {
  std::vector<std::string> fieldsOrder;
  fieldsOrder.reserve(this->getOperands().size() + 1);
  fieldsOrder.emplace_back("KEY");
  const auto &operands = this->getOperands();
  std::copy_if(operands.begin(), operands.end(), std::back_inserter(fieldsOrder),
               [](const auto &field) { return field != "KEY"; });

  std::vector<Table::FieldIndex> fieldIds;
  fieldIds.reserve(fieldsOrder.size() - 1);
  std::transform(fieldsOrder.begin() + 1, fieldsOrder.end(),
                 std::back_inserter(fieldIds),
                 [&table](const auto &field) {
                   return table.getFieldIndex(field);
                 });
  return fieldIds;
}

[[nodiscard]] QueryResult::Ptr SelectQuery::executeSingleThreaded(
    const Table &table, const std::vector<Table::FieldIndex> &fieldIds) {
  // Collect matching rows as pairs of (key, values)
  std::map<std::string, std::vector<Table::ValueType>> sorted_rows;

  for (auto it = table.begin(); it != table.end(); ++it) [[likely]]
  {
    if (this->evalCondition(*it)) [[likely]] {
      std::vector<Table::ValueType> values;
      values.reserve(fieldIds.size());
      std::transform(fieldIds.begin(), fieldIds.end(),
                     std::back_inserter(values),
                     [&it](const auto &field_id) { return (*it)[field_id]; });
      sorted_rows.emplace(it->key(), std::move(values));
    }
  }

  // Output in KEY order (already sorted by map)
  std::ostringstream buffer;
  
  // cppcheck-suppress unassignedVariable
  for (const auto &[key, values] : sorted_rows) [[likely]] {
    buffer << "( " << key;
    for (const auto &value : values) [[likely]]
    {
      buffer << " " << value;
    }
    buffer << " )\n";
  }

  return std::make_unique<TextRowsResult>(buffer.str());
}

[[nodiscard]] QueryResult::Ptr SelectQuery::executeMultiThreaded(
    const Table &table, const std::vector<Table::FieldIndex> &fieldIds) {
  constexpr size_t CHUNK_SIZE = Table::splitsize();
  const ThreadPool &pool = ThreadPool::getInstance();
  std::vector<std::future<std::map<std::string, std::vector<Table::ValueType>>>>
      futures;
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

    futures.push_back(pool.submit([this, chunk_begin, chunk_end, &fieldIds]() {
      std::map<std::string, std::vector<Table::ValueType>> local_rows;
      for (auto iter = chunk_begin; iter != chunk_end; ++iter) [[likely]]
      {
        if (this->evalCondition(*iter)) [[likely]] {
          std::vector<Table::ValueType> values;
          values.reserve(fieldIds.size());
          std::transform(
              fieldIds.begin(), fieldIds.end(), std::back_inserter(values),
              [&iter](const auto &field_id) { return (*iter)[field_id]; });
          local_rows.emplace(iter->key(), std::move(values));
        }
      }
      return local_rows;
    }));
  }

  // Merge all results into sorted map
  std::map<std::string, std::vector<Table::ValueType>> sorted_rows;
  for (auto &future : futures) [[likely]]
  {
    auto local_rows = future.get();
    sorted_rows.insert(local_rows.begin(), local_rows.end());
  }

  // Output in KEY order (already sorted by map)
  std::ostringstream buffer;
  for (const auto &[key, values] : sorted_rows) [[likely]] {
    buffer << "( " << key;
    for (const auto &value : values) [[likely]]
    {
      buffer << " " << value;
    }
    buffer << " )\n";
  }

  return std::make_unique<TextRowsResult>(buffer.str());
}
