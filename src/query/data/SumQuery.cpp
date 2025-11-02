#include "SumQuery.h"

#include <algorithm>
#include <cstddef>
#include <exception>
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

QueryResult::Ptr SumQuery::execute()
{
  Database& database = Database::getInstance();

  try
  {
    // Validate operands
    auto validation_result = validateOperands();
    if (validation_result != nullptr)
    {
      return validation_result;
    }

    auto lock = TableLockManager::getInstance().acquireRead(this->targetTable);
    auto& table = database[this->targetTable];

    // Get field indices
    auto fids = getFieldIndices(table);

    // Try key condition optimization first
    auto key_result = executeKeyConditionOptimization(table, fids);
    if (key_result != nullptr)
    {
      return key_result;
    }

    // Check if ThreadPool is available and has multiple threads
    if (!ThreadPool::isInitialized())
    {
      return executeSingleThreaded(table, fids);
    }

    ThreadPool& pool = ThreadPool::getInstance();
    if (pool.getThreadCount() <= 1 || table.size() < Table::splitsize())
    {
      return executeSingleThreaded(table, fids);
    }

    // Multi-threaded execution
    return executeMultiThreaded(table, fids);
  }
  catch (const TableNameNotFound&)
  {
    return std::make_unique<ErrorMsgResult>("SUM", this->targetTable, "No such table.");
  }
  catch (const TableFieldNotFound&)
  {
    return std::make_unique<ErrorMsgResult>("SUM", this->targetTable, "No such field.");
  }
  catch (const IllFormedQueryCondition& e)
  {
    return std::make_unique<ErrorMsgResult>("SUM", this->targetTable, e.what());
  }
  catch (const std::exception& e)
  {
    return std::make_unique<ErrorMsgResult>("SUM", this->targetTable,
                                            "Unknown error '?'"_f % e.what());
  }
}

std::string SumQuery::toString()
{
  return "QUERY = SUM \"" + this->targetTable + "\"";
}

QueryResult::Ptr SumQuery::validateOperands() const
{
  if (this->operands.empty())
  {
    return std::make_unique<ErrorMsgResult>("SUM", this->targetTable, "Invalid number of fields");
  }
  if (std::any_of(this->operands.begin(), this->operands.end(),
                  [](const auto& field) { return field == "KEY"; }))
  {
    return std::make_unique<ErrorMsgResult>("SUM", this->targetTable, "KEY cannot be summed.");
  }
  return nullptr;
}

std::vector<Table::FieldIndex> SumQuery::getFieldIndices(const Table& table) const
{
  std::vector<Table::FieldIndex> fids;
  fids.reserve(this->operands.size());
  for (const auto& field : this->operands)
  {
    fids.emplace_back(table.getFieldIndex(field));
  }
  return fids;
}