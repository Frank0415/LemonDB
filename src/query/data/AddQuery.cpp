#include "AddQuery.h"

#include <cstddef>
#include <exception>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "../../db/Database.h"
#include "../../db/TableLockManager.h"
#include "../../utils/formatter.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"
#include "threading/Threadpool.h"

[[nodiscard]] QueryResult::Ptr AddQuery::execute()
{
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
    // this operands stores a list of ADD ( fields ... destField ) FROM table
    // WHERE ( cond ) ...;
    // No proper conditions implemented

    // Stategy: iterate through all rows that satisfy the condition and sum all
    // @ once

    auto indices = getFieldIndices(table);

    if (!ThreadPool::isInitialized())
    {
      return executeSingleThreaded(table, indices);
    }

    const ThreadPool& pool = ThreadPool::getInstance();
    if (pool.getThreadCount() <= 1 || table.size() < Table::splitsize())
    {
      return executeSingleThreaded(table, indices);
    }

    return executeMultiThreaded(table, indices);
  }
  catch (const NotFoundKey& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            std::string("Key not found."));
  }
  catch (const TableNameNotFound& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            std::string("No such table."));
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

std::string AddQuery::toString()
{
  return "QUERY = ADD TABLE \"" + this->targetTableRef() + "\"";
}

[[nodiscard]] QueryResult::Ptr AddQuery::validateOperands() const
{
  if (this->getOperands().size() < 2)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            "Invalid number of operands (? operands)."_f %
                                                getOperands().size());
  }
  return nullptr;
}

[[nodiscard]] std::vector<Table::FieldIndex> AddQuery::getFieldIndices(const Table& table) const
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
AddQuery::executeSingleThreaded(Table& table, const std::vector<Table::FieldIndex>& fids)
{
  int count = 0;

  for (auto row : table)
  {
    if (!this->evalCondition(row))
    {
      continue;
    }
    // perform ADD operation
    int sum = 0;
    for (size_t idx = 0; idx < this->getOperands().size() - 1; ++idx)
    {
      sum += row[fids[idx]];
    }
    row[fids.back()] = sum;
    count++;
  }
  return std::make_unique<RecordCountResult>(count);
}

[[nodiscard]] QueryResult::Ptr
AddQuery::executeMultiThreaded(Table& table, const std::vector<Table::FieldIndex>& fids)
{
  constexpr size_t CHUNK_SIZE = Table::splitsize();
  const ThreadPool& pool = ThreadPool::getInstance();
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
            // perform ADD operation
            int sum = 0;
            for (size_t i = 0; i < this->getOperands().size() - 1; ++i)
            {
              sum += (*it)[fids[i]];
            }
            (*it)[fids.back()] = sum;
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