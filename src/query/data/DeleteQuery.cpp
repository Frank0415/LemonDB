
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

[[nodiscard]] QueryResult::Ptr DeleteQuery::executeMultiThreaded(Table& table)
{
  constexpr size_t CHUNK_SIZE = Table::splitsize();
  const ThreadPool& pool = ThreadPool::getInstance();
  std::vector<std::future<std::vector<Table::KeyType>>> futures;
  futures.reserve((table.size() + CHUNK_SIZE - 1) / CHUNK_SIZE);

  auto iterator = table.begin();
  while (iterator != table.end())
  {
    auto chunk_start = iterator;
    size_t count = 0;
    while (iterator != table.end() && count < CHUNK_SIZE)
    {
      ++iterator;
      ++count;
    }
    auto chunk_end = iterator;
    futures.push_back(pool.submit(
        [this, chunk_start, chunk_end]()
        {
          std::vector<Table::KeyType> local_keys;
          for (auto it = chunk_start; it != chunk_end; ++it)
          {
            if (this->evalCondition(*it))
            {
              local_keys.push_back(it->key());
            }
          }
          return local_keys;
        }));
  }

  // Collect all keys from parallel tasks
  std::vector<Table::KeyType> allKeysToDelete;
  for (auto& future : futures)
  {
    auto keys = future.get();
    allKeysToDelete.insert(allKeysToDelete.end(), keys.begin(), keys.end());
  }

  // Single-threaded deletion of all collected keys
  Table::SizeType counter = 0;
  for (const auto& key : allKeysToDelete)
  {
    table.deleteByIndex(key);
    ++counter;
  }

  return std::make_unique<RecordCountResult>(counter);
}

std::string DeleteQuery::toString()
{
  return "QUERY = DELETE " + this->targetTableRef() + "\"";
}
