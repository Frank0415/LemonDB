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
