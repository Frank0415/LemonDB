#include "MaxQuery.h"

#include <algorithm>
#include <cstddef>
#include <exception>
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

QueryResult::Ptr MaxQuery::execute()
{
  using std::string_literals::operator""s;

  try
  {
    if (validateOperands() != nullptr)
    {
      return validateOperands();
    }

    Database& database = Database::getInstance();
    auto lock = TableLockManager::getInstance().acquireRead(this->targetTable);
    auto& table = database[this->targetTable];

    auto result = initCondition(table);

    if (!result.second)
    {
      return std::make_unique<NullQueryResult>();
    }

    auto fieldId = getFieldIndices(table);

    if (!ThreadPool::isInitialized())
    {
      return executeSingleThreaded(table, fieldId);
    }

    ThreadPool& pool = ThreadPool::getInstance();

    if (pool.getThreadCount() <= 1 || table.size() < Table::splitsize())
    {
      return executeSingleThreaded(table, fieldId);
    }

    return std::make_unique<NullQueryResult>();
  }
  catch (const TableNameNotFound& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable, "No such table."s);
  }
  catch (const TableFieldNotFound& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
  }
  catch (const IllFormedQueryCondition& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
  }
  catch (const std::invalid_argument& e)
  {
    // Cannot convert operand to string
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable,
                                            "Unknown error '?'"_f % e.what());
  }
  catch (const std::exception& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable,
                                            "Unknown error '?'."_f % e.what());
  }
}

std::string MaxQuery::toString()
{
  return "QUERY = MAX " + this->targetTable + "\"";
}

[[nodiscard]] QueryResult::Ptr MaxQuery::validateOperands() const
{
  if (this->operands.empty())
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable.c_str(),
                                            "No operand (? operands)."_f % operands.size());
  }
  return nullptr;
}

[[nodiscard]] std::vector<Table::FieldIndex> MaxQuery::getFieldIndices(const Table& table) const
{
  std::vector<Table::FieldIndex> fieldId;
  for (const auto& operand : this->operands)
  {
    if (operand == "KEY")
    {
      throw IllFormedQueryCondition("MAX operation not supported on KEY field.");
    }
    fieldId.push_back(table.getFieldIndex(operand));
  }
  return fieldId;
}


[[nodiscard]] QueryResult::Ptr
MaxQuery::executeSingleThreaded(Table& table, const std::vector<Table::FieldIndex>& fids)
{
  bool found = false;
  std::vector<Table::ValueType> maxValue(fids.size(),
                                         Table::ValueTypeMin); // each has its own max value

  for (const auto& row : table)
  {
    if (this->evalCondition(row))
    {
      found = true;

      for (size_t i = 0; i < fids.size(); ++i)
      {
        maxValue[i] = std::max(maxValue[i], row[fids[i]]);
      }
    }
  }

  if (!found)
  {
    return std::make_unique<NullQueryResult>();
  }
  return std::make_unique<SuccessMsgResult>(maxValue);
}

[[nodiscard]] QueryResult::Ptr
MaxQuery::executeMultiThreaded(Table& table, const std::vector<Table::FieldIndex>& fids)
{
  // Multi-threaded execution logic
  return nullptr;
}
