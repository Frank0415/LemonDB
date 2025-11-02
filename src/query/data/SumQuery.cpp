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

QueryResult::Ptr
SumQuery::executeKeyConditionOptimization(Table& table, const std::vector<Table::FieldIndex>& fids)
{
  const size_t num_fields = fids.size();
  std::vector<Table::ValueType> sums(num_fields, 0);

  const bool handled = this->testKeyCondition(table,
                                              [&](bool success, Table::Object::Ptr obj)
                                              {
                                                if (!success || !obj)
                                                {
                                                  return;
                                                }
                                                for (size_t i = 0; i < num_fields; ++i)
                                                {
                                                  sums[i] += (*obj)[fids[i]];
                                                }
                                              });
  if (handled)
  {
    return std::make_unique<SuccessMsgResult>(sums);
  }
  return nullptr;
}

QueryResult::Ptr SumQuery::executeSingleThreaded(Table& table,
                                                 const std::vector<Table::FieldIndex>& fids)
{
  const size_t num_fields = fids.size();
  std::vector<Table::ValueType> sums(num_fields, 0);

  for (auto it = table.begin(); it != table.end(); ++it)
  {
    if (this->evalCondition(*it))
    {
      for (size_t i = 0; i < num_fields; ++i)
      {
        sums[i] += (*it)[fids[i]];
      }
    }
  }
  return std::make_unique<SuccessMsgResult>(sums);
}

QueryResult::Ptr SumQuery::executeMultiThreaded(Table& table,
                                                const std::vector<Table::FieldIndex>& fids)
{
  constexpr size_t CHUNK_SIZE = 2000;
  ThreadPool& pool = ThreadPool::getInstance();
  const size_t num_fields = fids.size();
  std::vector<Table::ValueType> sums(num_fields, 0);

  // Create chunks and submit tasks
  std::vector<std::future<std::vector<Table::ValueType>>> futures;
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
        [this, fids, chunk_begin, chunk_end, num_fields]()
        {
          std::vector<Table::ValueType> local_sums(num_fields, 0);
          for (auto iter = chunk_begin; iter != chunk_end; ++iter)
          {
            if (this->evalCondition(*iter))
            {
              for (size_t i = 0; i < num_fields; ++i)
              {
                local_sums[i] += (*iter)[fids[i]];
              }
            }
          }
          return local_sums;
        }));
  }

  // Combine results from all threads
  for (auto& future : futures)
  {
    auto local_sums = future.get();
    for (size_t i = 0; i < num_fields; ++i)
    {
      sums[i] += local_sums[i];
    }
  }

  return std::make_unique<SuccessMsgResult>(sums);
}
