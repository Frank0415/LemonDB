#include "SelectQuery.h"

#include <algorithm>
#include <cstddef>
#include <exception>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

#include "../../db/Database.h"
#include "../../db/Table.h"
#include "../../db/TableLockManager.h"
#include "../../threading/Threadpool.h"
#include "../../utils/formatter.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"

QueryResult::Ptr SelectQuery::execute()
{
  try
  {
    auto& database = Database::getInstance();
    auto lock = TableLockManager::getInstance().acquireRead(this->targetTable);
    auto& table = database[this->targetTable];
    if (this->operands.empty())
    {
      return std::make_unique<ErrorMsgResult>(qname, this->targetTable, "Invalid operands.");
    }
    std::vector<std::string> fieldsOrder;
    fieldsOrder.reserve(this->operands.size() + 1);
    fieldsOrder.emplace_back("KEY");
    for (const auto& field : this->operands)
    {
      if (field != "KEY")
      {
        fieldsOrder.emplace_back(field);
      }
    }
    std::vector<Table::FieldIndex> fieldIds;
    fieldIds.reserve(fieldsOrder.size() - 1);
    for (size_t i = 1; i < fieldsOrder.size(); ++i)
    {
      fieldIds.emplace_back(table.getFieldIndex(fieldsOrder[i]));
    }

    std::ostringstream buffer;
    const bool handled = this->testKeyCondition(table,
                                                [&](bool success, Table::Object::Ptr&& obj)
                                                {
                                                  if (!success)
                                                  {
                                                    return;
                                                  }
                                                  if (obj)
                                                  {
                                                    buffer << "( " << obj->key();
                                                    for (const auto& field_id : fieldIds)
                                                    {
                                                      buffer << " " << (*obj)[field_id];
                                                    }
                                                    buffer << " )\n";
                                                  }
                                                });
    if (handled)
    {
      return std::make_unique<TextRowsResult>(buffer.str());
    }

    std::vector<Table::Iterator> rows;
    for (auto it = table.begin(); it != table.end(); ++it)
    {
      if (this->evalCondition(*it))
      {
        rows.push_back(it);
      }
    }
    std::sort(rows.begin(), rows.end(),
              [](const Table::Iterator& first, const Table::Iterator& second)
              { return (*first).key() < (*second).key(); });

    for (const auto& iterator : rows)
    {
      buffer << "( " << (*iterator).key();
      for (const auto& field_id : fieldIds)
      {
        buffer << " " << (*iterator)[field_id];
      }
      buffer << " )\n";
    }
    return std::make_unique<TextRowsResult>(buffer.str());
  }
  catch (const TableNameNotFound&)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable, "No such table.");
  }
  catch (const IllFormedQueryCondition& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
  }
  catch (const std::exception& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable,
                                            "Unknown error '?'."_f % e.what());
  }
}

std::string SelectQuery::toString()
{
  return "QUERY = SELECT \"" + this->targetTable + "\"";
}

[[nodiscard]] QueryResult::Ptr SelectQuery::validateOperands() const
{
  if (this->operands.empty())
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable, "Invalid operands.");
  }
  return nullptr;
}

[[nodiscard]] std::vector<Table::FieldIndex> SelectQuery::getFieldIndices(const Table& table) const
{
  std::vector<std::string> fieldsOrder;
  fieldsOrder.reserve(this->operands.size() + 1);
  fieldsOrder.emplace_back("KEY");
  for (const auto& field : this->operands)
  {
    if (field != "KEY")
    {
      fieldsOrder.emplace_back(field);
    }
  }

  std::vector<Table::FieldIndex> fieldIds;
  fieldIds.reserve(fieldsOrder.size() - 1);
  for (size_t i = 1; i < fieldsOrder.size(); ++i)
  {
    fieldIds.emplace_back(table.getFieldIndex(fieldsOrder[i]));
  }
  return fieldIds;
}

[[nodiscard]] QueryResult::Ptr
SelectQuery::executeSingleThreaded(const Table& table,
                                   const std::vector<Table::FieldIndex>& fieldIds)
{
  // Collect matching rows as pairs of (key, values)
  std::map<std::string, std::vector<Table::ValueType>> sorted_rows;

  for (auto it = table.begin(); it != table.end(); ++it)
  {
    if (this->evalCondition(*it))
    {
      std::vector<Table::ValueType> values;
      values.reserve(fieldIds.size());
      for (const auto& field_id : fieldIds)
      {
        values.emplace_back((*it)[field_id]);
      }
      sorted_rows.emplace(it->key(), std::move(values));
    }
  }

  // Output in KEY order (already sorted by map)
  std::ostringstream buffer;
  for (const auto& [key, values] : sorted_rows)
  {
    buffer << "( " << key;
    for (const auto& value : values)
    {
      buffer << " " << value;
    }
    buffer << " )\n";
  }

  return std::make_unique<TextRowsResult>(buffer.str());
}

[[nodiscard]] QueryResult::Ptr
SelectQuery::executeMultiThreaded(const Table& table,
                                  const std::vector<Table::FieldIndex>& fieldIds)
{
  constexpr size_t CHUNK_SIZE = Table::splitsize();
  ThreadPool& pool = ThreadPool::getInstance();
  std::vector<std::future<std::map<std::string, std::vector<Table::ValueType>>>> futures;
  futures.reserve((table.size() + CHUNK_SIZE - 1) / CHUNK_SIZE);

  auto iterator = table.begin();
  while (iterator != table.end())
  {
    auto chunk_begin = iterator;
    size_t count = 0;
    while (iterator != table.end() && count < CHUNK_SIZE)
    {
      ++iterator;
      ++count;
    }
    auto chunk_end = iterator;

    futures.push_back(pool.submit(
        [this, chunk_begin, chunk_end, &fieldIds]()
        {
          std::map<std::string, std::vector<Table::ValueType>> local_rows;
          for (auto iter = chunk_begin; iter != chunk_end; ++iter)
          {
            if (this->evalCondition(*iter))
            {
              std::vector<Table::ValueType> values;
              values.reserve(fieldIds.size());
              for (const auto& field_id : fieldIds)
              {
                values.emplace_back((*iter)[field_id]);
              }
              local_rows.emplace(iter->key(), std::move(values));
            }
          }
          return local_rows;
        }));
  }

  // Merge all results into sorted map
  std::map<std::string, std::vector<Table::ValueType>> sorted_rows;
  for (auto& future : futures)
  {
    auto local_rows = future.get();
    sorted_rows.insert(local_rows.begin(), local_rows.end());
  }

  // Output in KEY order (already sorted by map)
  std::ostringstream buffer;
  for (const auto& [key, values] : sorted_rows)
  {
    buffer << "( " << key;
    for (const auto& value : values)
    {
      buffer << " " << value;
    }
    buffer << " )\n";
  }

  return std::make_unique<TextRowsResult>(buffer.str());
}
