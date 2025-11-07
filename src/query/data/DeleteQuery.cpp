
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
#include "../../utils/formatter.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"
#include "DeleteQuery.h"
#include "threading/Threadpool.h"

[[nodiscard]] QueryResult::Ptr DeleteQuery::execute()
{
  if (!this->getOperands().empty())
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef().c_str(),
                                            "Invalid number of operands (? operands)."_f %
                                                getOperands().size());
  }

  try
  {
    auto& database = Database::getInstance();
    auto lock = TableLockManager::getInstance().acquireWrite(this->targetTableRef());
    auto& table = database[this->targetTableRef()];
    auto result = initCondition(table);
    if (!result.second)
    {
      throw IllFormedQueryCondition("Error conditions in WHERE clause.");
    }

    if (!ThreadPool::isInitialized())
    {
      return executeSingleThreaded(table);
    }

    const ThreadPool& pool = ThreadPool::getInstance();
    if (pool.getThreadCount() <= 1 || table.size() < Table::splitsize())
    {
      return executeSingleThreaded(table);
    }

    return executeMultiThreaded(table);
  }
  catch (const NotFoundKey& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(), "Key not found.");
  }
  catch (const TableNameNotFound& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(), "No such table.");
  }
  catch (const IllFormedQueryCondition& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(), e.what());
  }
  catch (const std::invalid_argument& e)
  {
    // Cannot convert operand to string
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            "Unknown error '?'"_f % e.what());
  }
  catch (const std::exception& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            "Unkonwn error '?'."_f % e.what());
  }
}

[[nodiscard]] QueryResult::Ptr DeleteQuery::executeSingleThreaded(Table& table)
{
  Table::SizeType counter = 0;
  std::vector<Table::KeyType> keysToDelete;

  // Single-threaded collection of keys to delete
  for (auto it = table.begin(); it != table.end(); ++it)
  {
    if (this->evalCondition(*it))
    {
      keysToDelete.push_back(it->key());
      ++counter;
    }
  }

  // Single-threaded deletion of collected keys
  for (const auto& key : keysToDelete)
  {
    table.deleteByIndex(key);
  }

  return std::make_unique<RecordCountResult>(counter);
}

std::string DeleteQuery::toString()
{
  return "QUERY = DELETE " + this->targetTableRef() + "\"";
}
