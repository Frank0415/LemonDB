#include "MinQuery.h"

#include <algorithm>
#include <cstddef>
#include <exception>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "../../db/Database.h"
#include "../../db/Table.h"
#include "../../db/TableLockManager.h"
#include "../../threading/Threadpool.h"
#include "../../utils/formatter.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"

QueryResult::Ptr MinQuery::execute()
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
    return executeMultiThreaded(table, fieldId);

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

std::string MinQuery::toString()
{
  return "QUERY = MIN " + this->targetTable + "\"";
}

[[nodiscard]] QueryResult::Ptr MinQuery::validateOperands() const
{
  if (this->getOperands().empty())
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable.c_str(),
                                            "No operand (? operands)."_f % getOperands().size());
  }
  return nullptr;
}

[[nodiscard]] std::vector<Table::FieldIndex> MinQuery::getFieldIndices(const Table& table) const
{
  std::vector<Table::FieldIndex> fieldId;
  for (const auto& operand : this->getOperands())
  {
    if (operand == "KEY")
    {
      throw IllFormedQueryCondition("MIN operation not supported on KEY field.");
    }
    fieldId.push_back(table.getFieldIndex(operand));
  }
  return fieldId;
}

[[nodiscard]] QueryResult::Ptr
MinQuery::executeSingleThreaded(Table& table, const std::vector<Table::FieldIndex>& fids)
{
  bool found = false;
  std::vector<Table::ValueType> minValue(fids.size(),
                                         Table::ValueTypeMax); // each has its own min value

  for (const auto& row : table)
  {
    if (this->evalCondition(row))
    {
      found = true;

      for (size_t i = 0; i < fids.size(); ++i)
      {
        minValue[i] = std::min(minValue[i], row[fids[i]]);
      }
    }
  }

  if (!found)
  {
    return std::make_unique<NullQueryResult>();
  }
  return std::make_unique<SuccessMsgResult>(minValue);
}

[[nodiscard]] QueryResult::Ptr
MinQuery::executeMultiThreaded(Table& table, const std::vector<Table::FieldIndex>& fids)
{
  constexpr size_t CHUNK_SIZE = Table::splitsize();
  ThreadPool& pool = ThreadPool::getInstance();
  const size_t num_fields = fids.size();
  std::vector<Table::ValueType> minValues(num_fields, Table::ValueTypeMax);

  // Create chunks and submit tasks
  std::vector<std::future<std::vector<Table::ValueType>>> futures;
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
        [this, fids, chunk_begin, chunk_end, num_fields]()
        {
          std::vector<Table::ValueType> local_min(num_fields, Table::ValueTypeMax);
          for (auto it = chunk_begin; it != chunk_end; ++it)
          {
            if (this->evalCondition(*it))
            {
              for (size_t i = 0; i < num_fields; ++i)
              {
                local_min[i] = std::min(local_min[i], (*it)[fids[i]]);
              }
            }
          }
          return local_min;
        }));
  }
  bool any_found = false;
  for (auto& future : futures)
  {
    auto local_min = future.get();
    for (size_t i = 0; i < num_fields; ++i)
    {
      if (!any_found && local_min[i] != Table::ValueTypeMax)
      {
        any_found = true;
      }
      minValues[i] = std::min(minValues[i], local_min[i]);
    }
  }
  if (!any_found)
  {
    return std::make_unique<NullQueryResult>();
  }
  return std::make_unique<SuccessMsgResult>(minValues);
}
