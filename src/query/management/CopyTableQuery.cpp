#include "CopyTableQuery.h"

#include <cstddef>
#include <exception>
#include <future>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../../db/Database.h"
#include "../../db/Table.h"
#include "../../db/TableLockManager.h"
#include "../../threading/Threadpool.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"

QueryResult::Ptr CopyTableQuery::execute()
{
  try
  {
    auto& database = Database::getInstance();
    auto srcLock = TableLockManager::getInstance().acquireRead(this->targetTable);
    auto dstLock = TableLockManager::getInstance().acquireWrite(this->newTableName);
    auto& src = database[this->targetTable];
    bool targetExists = false;
    try
    {
      (void)database[this->newTableName];
      targetExists = true;
    }
    catch (...)
    {
      // Intentionally ignore: table doesn't exist, which is expected
      (void)0;
    }
    if (targetExists)
    {
      return std::make_unique<ErrorMsgResult>(qname, this->targetTable, "Target table name exists");
    }

    const std::vector<std::string> fields = src.field();
    auto dup = std::make_unique<Table>(this->newTableName, fields);
    for (const auto& obj : src)
    {
      std::vector<Table::ValueType> row;
      row.reserve(fields.size());
      for (size_t i = 0; i < fields.size(); ++i)
      {
        row.push_back(obj[i]);
      }
      dup->insertByIndex(obj.key(), std::move(row));
    }

    database.registerTable(std::move(dup));
    return std::make_unique<NullQueryResult>();
  }
  catch (const TableNameNotFound&)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable, "No such table.");
  }
  catch (const std::exception& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable, "Unknown error");
  }
}

std::string CopyTableQuery::toString()
{
  return "QUERY = COPYTABLE, SOURCE = \"" + this->targetTable + "\", TARGET = \"" +
         this->newTableName + "\"";
}

[[nodiscard]] QueryResult::Ptr CopyTableQuery::validateSourceTable(const Table& src) const
{
  if (src.empty())
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable, "Source table is empty.");
  }
  return nullptr;
}

[[nodiscard]] std::vector<CopyTableQuery::RowData>
CopyTableQuery::collectSingleThreaded(const Table& src, const std::vector<std::string>& fields)
{
  std::vector<RowData> results;
  results.reserve(src.size());

  for (const auto& obj : src)
  {
    std::vector<Table::ValueType> row;
    row.reserve(fields.size());
    for (size_t i = 0; i < fields.size(); ++i)
    {
      row.push_back(obj[i]);
    }
    results.emplace_back(obj.key(), std::move(row));
  }

  return results;
}

[[nodiscard]] std::vector<CopyTableQuery::RowData>
CopyTableQuery::collectMultiThreaded(const Table& src, const std::vector<std::string>& fields)
{
  constexpr size_t CHUNK_SIZE = Table::splitsize();
  ThreadPool& pool = ThreadPool::getInstance();
  std::vector<RowData> results;

  // Create chunks and submit tasks
  std::vector<std::pair<size_t, std::future<std::vector<RowData>>>> tasks;
  tasks.reserve((src.size() + CHUNK_SIZE - 1) / CHUNK_SIZE);

  auto iterator = src.begin();
  size_t chunk_index = 0;
  while (iterator != src.end())
  {
    auto chunk_begin = iterator;
    size_t count = 0;
    while (iterator != src.end() && count < CHUNK_SIZE)
    {
      ++iterator;
      ++count;
    }
    auto chunk_end = iterator;

    tasks.emplace_back(chunk_index,
                       pool.submit(
                           [chunk_begin, chunk_end, &fields]()
                           {
                             std::vector<RowData> local_results;
                             local_results.reserve(std::distance(chunk_begin, chunk_end));
                             for (auto iter = chunk_begin; iter != chunk_end; ++iter)
                             {
                               std::vector<Table::ValueType> row;
                               row.reserve(fields.size());
                               for (size_t i = 0; i < fields.size(); ++i)
                               {
                                 row.push_back((*iter)[i]);
                               }
                               local_results.emplace_back(iter->key(), std::move(row));
                             }
                             return local_results;
                           }));
    ++chunk_index;
  }

  // Collect results in order by chunk_index
  for (auto& [idx, future] : tasks)
  {
    auto local_results = future.get();
    results.insert(results.end(), std::make_move_iterator(local_results.begin()),
                   std::make_move_iterator(local_results.end()));
  }

  return results;
}
