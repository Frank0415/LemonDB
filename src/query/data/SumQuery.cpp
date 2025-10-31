#include "SumQuery.h"

#include <algorithm>
#include <cstddef>
#include <exception>
#include <future>
#include <memory>
#include <string>
#include <vector>

#include "../../db/Database.h"
#include "../../db/Table.h"
#include "../../db/TableLockManager.h"
#include "../../query/QueryResult.h"
#include "../../query/data/SumQuery.h"
#include "../../threading/Threadpool.h"
#include "../../utils/formatter.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"

QueryResult::Ptr SumQuery::execute()
{
  Database& db = Database::getInstance();

  try
  {
    auto lock = TableLockManager::getInstance().acquireRead(this->targetTable);
    auto& table = db[this->targetTable];
    if (this->operands.empty())
    {
      return std::make_unique<ErrorMsgResult>("SUM", this->targetTable, "Invalid number of fields");
    }
    // Check if any operand is "KEY" using std::any_of
    if (std::any_of(this->operands.begin(), this->operands.end(),
                    [](const auto& f) { return f == "KEY"; }))
    {
      return std::make_unique<ErrorMsgResult>("SUM", this->targetTable, "KEY cannot be summed.");
    }
    std::vector<Table::FieldIndex> fids;
    fids.reserve(this->operands.size());
    for (const auto& f : this->operands)
    {
      fids.emplace_back(table.getFieldIndex(f));
    }

    const size_t num_fields = fids.size();
    std::vector<Table::ValueType> sums(num_fields, 0);

    // Try to use testKeyCondition optimization first
    const bool handled = this->testKeyCondition(table,
                                                [&](bool ok, Table::Object::Ptr&& obj)
                                                {
                                                  if (!ok)
                                                  {
                                                    return;
                                                  }
                                                  if (obj)
                                                  {
                                                    for (size_t i = 0; i < num_fields; ++i)
                                                    {
                                                      sums[i] += (*obj)[fids[i]];
                                                    }
                                                  }
                                                });
    if (handled)
    {
      return std::make_unique<SuccessMsgResult>(sums);
    }

    // Check if ThreadPool is available and has multiple threads
    if (!ThreadPool::isInitialized())
    {
      // Single-threaded fallback
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

    ThreadPool& pool = ThreadPool::getInstance();
    if (pool.getThreadCount() <= 1 || table.size() < 2000)
    {
      // Single-threaded for small tables or single thread pool
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

    // Multi-threaded execution
    const size_t chunk_size = 2000;

    // Reset sums for multi-threaded accumulation
    std::fill(sums.begin(), sums.end(), 0);

    std::vector<std::future<std::vector<Table::ValueType>>> futures;
    futures.reserve((table.size() + chunk_size - 1) / chunk_size);

    auto iterator = table.begin();
    while (iterator != table.end())
    {
      auto chunk_begin = iterator;
      size_t count = 0;
      while (iterator != table.end() && count < chunk_size)
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
