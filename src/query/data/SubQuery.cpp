#include "SubQuery.h"

#include <cstddef>
#include <exception>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>

#include "../../db/Database.h"
#include "../../db/TableLockManager.h"
#include "../../threading/Threadpool.h"
#include "../../utils/formatter.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"

QueryResult::Ptr SubQuery::execute()
{
  using std::string_literals::operator""s;
  try
  {
    auto validationResult = validateOperands();
    if (validationResult != nullptr)
    {
      return validationResult;
    }
    auto& database = Database::getInstance();
    auto lock = TableLockManager::getInstance().acquireWrite(this->targetTable);
    auto& table = database[this->targetTable];

    auto result = initCondition(table);
    if (!result.second)
    {
      return std::make_unique<RecordCountResult>(0);
    }

    auto indices = getFieldIndices(table);

    if (!ThreadPool::isInitialized())
    {
      return executeSingleThreaded(table, indices);
    }

    ThreadPool& pool = ThreadPool::getInstance();
    if (pool.getThreadCount() <= 1 || table.size() < Table::splitsize())
    {
      return executeSingleThreaded(table, indices);
    }

    return executeMultiThreaded(table, indices);
  }
  catch (const NotFoundKey& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable, "Key not found."s);
  }
  catch (const TableNameNotFound& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable, "No such table."s);
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
                                            "Unkonwn error '?'."_f % e.what());
  }
}

std::string SubQuery::toString()
{
  return "QUERY = SUB TABLE \"" + this->targetTable + "\"";
}
