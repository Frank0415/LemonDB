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
    auto lock = TableLockManager::getInstance().acquireWrite(this->targetTableRef());
    auto& table = database[this->targetTableRef()];

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
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(), "Key not found."s);
  }
  catch (const TableNameNotFound& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(), "No such table."s);
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

std::string SubQuery::toString()
{
  return "QUERY = SUB TABLE \"" + this->targetTableRef() + "\"";
}

[[nodiscard]] QueryResult::Ptr SubQuery::validateOperands() const
{
  if (this->getOperands().size() < 2)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            "Invalid number of operands (? operands)."_f %
                                                getOperands().size());
  }
  return nullptr;
}

[[nodiscard]] std::vector<Table::FieldIndex> SubQuery::getFieldIndices(const Table& table) const
{
  std::vector<Table::FieldIndex> indices;
  indices.reserve(this->getOperands().size());
  for (const auto& operand : this->getOperands())
  {
    indices.push_back(table.getFieldIndex(operand));
  }
  return indices;
}

[[nodiscard]] QueryResult::Ptr
SubQuery::executeSingleThreaded(Table& table, const std::vector<Table::FieldIndex>& fids)
{
  int count = 0;

  for (auto it = table.begin(); it != table.end(); ++it)
  {
    if (!this->evalCondition(*it))
    {
      continue;
    }
    // perform SUB operation
    int diff = (*it)[fids[0]];
    for (size_t i = 1; i < this->getOperands().size() - 1; ++i)
    {
      diff -= (*it)[fids[i]];
    }
    (*it)[fids.back()] = diff;
    count++;
  }
  return std::make_unique<RecordCountResult>(count);
}

[[nodiscard]] QueryResult::Ptr
SubQuery::executeMultiThreaded(Table& table, const std::vector<Table::FieldIndex>& fids)
{
  constexpr size_t CHUNK_SIZE = Table::splitsize();
  ThreadPool& pool = ThreadPool::getInstance();
  std::vector<std::future<int>> futures;
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
        [this, chunk_start, chunk_end, fids]()
        {
          int local_count = 0;
          for (auto it = chunk_start; it != chunk_end; ++it)
          {
            if (!this->evalCondition(*it))
            {
              continue;
            }
            // perform SUB operation
            int diff = (*it)[fids[0]];
            for (size_t i = 1; i < this->getOperands().size() - 1; ++i)
            {
              diff -= (*it)[fids[i]];
            }
            (*it)[fids.back()] = diff;
            local_count++;
          }
          return local_count;
        }));
  }

  // Wait for all tasks to complete and aggregate results
  int total_count = 0;
  for (auto& future : futures)
  {
    total_count += future.get();
  }
  return std::make_unique<RecordCountResult>(total_count);
}
