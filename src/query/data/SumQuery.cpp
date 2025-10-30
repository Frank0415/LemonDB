#include "query/data/SumQuery.h"
#include "db/Database.h"
#include "db/TableLockManager.h"
#include "query/QueryResult.h"
#include "threading/Threadpool.h"

#include "SumQuery.h"

#include <algorithm>
#include <cstddef>
#include <exception>
#include <memory>
#include <string>
#include <vector>

#include "../../db/Database.h"
#include "../../db/Table.h"
#include "../../query/QueryResult.h"
#include "../../query/data/SumQuery.h"
#include "../../utils/formatter.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"

QueryResult::Ptr SumQuery::execute()
{

  try
  {
    Database& database = Database::getInstance();
    auto& table = database[this->targetTable];

    if (this->operands.empty())
    {
      return make_unique<ErrorMsgResult>("SUM", this->targetTable, "Invalid number of fields");
    }
    for (const auto& f : this->operands)
    {
      if (f == "KEY")
      {
        return make_unique<ErrorMsgResult>("SUM", this->targetTable, "KEY cannot be summed.");
      }
    }
    auto result = initCondition(table);
    if (!result.second)
    {
      throw IllFormedQueryCondition("Error conditions in WHERE clause.");
    }
    std::vector<Table::FieldIndex> fids;
    fids.reserve(this->operands.size());
    for (const auto& field : this->operands)
    {
      fids.emplace_back(table.getFieldIndex(field));
    }

    // NEW: LOCK IMPL
    auto& lockmgr = TableLockManager::getInstance();
    auto lock = lockmgr.acquireRead(this->targetTable);

    // NEW: READ LOCK PROTECTED MODE
    static ThreadPool& pool = ThreadPool::getInstance();
    const size_t chunk_size = 20000;
    const size_t num_fields = fids.size();

    if (pool.getThreadCount() <= 1)
    {
      std::vector<int> sums(num_fields, 0);
      bool handled = this->testKeyCondition(table,
                                            [&](bool is_valid, Table::Object::Ptr&& obj)
                                            {
                                              if (!is_valid || !obj)
                                              {
                                                return;
                                              }
                                              for (size_t i = 0; i < num_fields; ++i)
                                              {
                                                sums[i] += (*obj)[fids[i]];
                                              }
                                            });

      if (!handled)
      {
        for (auto iterator = table.begin(); iterator != table.end(); ++iterator)
        {
          if (this->evalCondition(*iterator))
          {
            for (size_t i = 0; i < num_fields; ++i)
            {
              sums[i] += (*iterator)[fids[i]];
            }
          }
        }
      }

      return make_unique<SuccessMsgResult>(sums);
    }

    std::vector<std::future<std::vector<int>>> futures;
    futures.reserve((table.size() + chunk_size - 1) / chunk_size);

    // NEEDS ANOTHER SET OF LOGIC FOR TABLE < 2000

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
            std::vector<int> local_sums(num_fields, 0);
            for (auto iterator = chunk_begin; iterator != chunk_end; ++iterator)
            {
              if (this->evalCondition(*iterator))
              {
                for (size_t i = 0; i < num_fields; ++i)
                {
                  local_sums[i] += (*iterator)[fids[i]];
                }
              }
            }
            return local_sums;
          }));
    }

    std::vector<int> sums(num_fields, 0);
    for (size_t i = 0; i < futures.size(); ++i)
    {
      auto local_sums = futures[i].get();
      for (size_t j = 0; j < num_fields; ++j)
      {
        sums[j] += local_sums[j];
      }
    }

    return make_unique<SuccessMsgResult>(sums);
  }
  catch (const TableNameNotFound&)
  {
    return make_unique<ErrorMsgResult>("SUM", this->targetTable, "No such table.");
  }
  catch (const TableFieldNotFound&)
  {
    return make_unique<ErrorMsgResult>("SUM", this->targetTable, "No such field.");
  }
  catch (const IllFormedQueryCondition& e)
  {
    return make_unique<ErrorMsgResult>("SUM", this->targetTable, e.what());
  }
  catch (const std::exception& e)
  {
    return make_unique<ErrorMsgResult>("SUM", this->targetTable, "Unknown error '?'"_f % e.what());
  }
}

std::string SumQuery::toString()
{
  return "QUERY = SUM \"" + this->targetTable + "\"";
}
