#include "DuplicateQuery.h"

#include <cstddef>
#include <exception>
#include <future>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "../../db/Database.h"
#include "../../db/Table.h"
#include "../../db/TableLockManager.h"
#include "../../threading/Threadpool.h"
#include "../../utils/formatter.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"

QueryResult::Ptr DuplicateQuery::execute()
{
  try
  {
    auto validationResult = validateOperands();
    if (validationResult != nullptr) [[unlikely]]
    {
      return validationResult;
    }

    auto& database = Database::getInstance();
    auto lock = TableLockManager::getInstance().acquireWrite(this->targetTableRef());
    auto& table = database[this->targetTableRef()];

    auto result = initCondition(table);
    if (!result.second) [[unlikely]]
    {
      throw IllFormedQueryCondition("Error conditions in WHERE clause.");
    }

    // Decide between single-threaded and multi-threaded execution
    std::vector<RecordPair> recordsToDuplicate;

    if (!ThreadPool::isInitialized()) [[unlikely]]
    {
      recordsToDuplicate = executeSingleThreaded(table);
    }
    else [[likely]]
    {
      const ThreadPool& pool = ThreadPool::getInstance();
      if (pool.getThreadCount() <= 1 || table.size() < Table::splitsize()) [[unlikely]]
      {
        recordsToDuplicate = executeSingleThreaded(table);
      }
      else [[likely]]
      {
        recordsToDuplicate = executeMultiThreaded(table);
      }
    }

    // Insert all duplicated records (in order preserved by helpers)
    Table::SizeType counter = 0;
    for (auto& record : recordsToDuplicate) [[likely]]
    {
      try
      {
        table.insertByIndex(record.first, std::move(record.second));
        ++counter;
      }
      catch (const ConflictingKey& e)
      {
        // Intentionally ignore: if key conflict exists, skip this record
        (void)e;
      }
    }

    return std::make_unique<RecordCountResult>(counter);
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

std::string DuplicateQuery::toString()
{
  return "QUERY = DUPLICATE " + this->targetTableRef() + "\"";
}

[[nodiscard]] QueryResult::Ptr DuplicateQuery::validateOperands() const
{
  if (!this->getOperands().empty()) [[unlikely]]
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            "Invalid number of operands (? operands)."_f %
                                                getOperands().size());
  }
  return nullptr;
}

[[nodiscard]] std::vector<DuplicateQuery::RecordPair>
DuplicateQuery::executeSingleThreaded(Table& table)
{
  std::vector<RecordPair> recordsToDuplicate;
  auto row_size = table.field().size();

  for (auto it = table.begin(); it != table.end(); ++it) [[likely]]
  {
    if (!this->evalCondition(*it)) [[unlikely]]
    {
      continue;
    }

    auto originalKey = it->key();
    auto newKey = originalKey + "_copy";

    // if a "_copy" already exists, skip this key
    if (table[newKey] != nullptr) [[unlikely]]
    {
      continue;
    }

    // Copy the values from the original record
    std::vector<Table::ValueType> values(row_size);
    for (size_t i = 0; i < row_size; ++i) [[likely]]
    {
      values[i] = (*it)[i];
    }

    recordsToDuplicate.emplace_back(newKey, std::move(values));
  }

  return recordsToDuplicate;
}

[[nodiscard]] std::vector<DuplicateQuery::RecordPair>
DuplicateQuery::executeMultiThreaded(Table& table)
{
  constexpr size_t CHUNK_SIZE = Table::splitsize();
  const ThreadPool& pool = ThreadPool::getInstance();
  auto row_size = table.field().size();

  // Collect tasks with their positions to preserve order
  std::vector<std::pair<size_t, std::future<std::vector<RecordPair>>>> tasks;
  tasks.reserve((table.size() + CHUNK_SIZE - 1) / CHUNK_SIZE);

  size_t chunk_index = 0;
  auto iterator = table.begin();
  while (iterator != table.end()) [[likely]]
  {
    auto chunk_begin = iterator;
    size_t count = 0;
    while (iterator != table.end() && count < CHUNK_SIZE) [[likely]]
    {
      ++iterator;
      ++count;
    }
    auto chunk_end = iterator;

    const size_t current_chunk_index = chunk_index;
    tasks.emplace_back(current_chunk_index, pool.submit(
                                                [this, &table, chunk_begin, chunk_end, row_size]()
                                                {
                                                  std::vector<RecordPair> local_records;
                                                  for (auto it = chunk_begin; it != chunk_end; ++it) [[likely]]
                                                  {
                                                    if (!this->evalCondition(*it)) [[unlikely]]
                                                    {
                                                      continue;
                                                    }

                                                    auto originalKey = it->key();
                                                    auto newKey = originalKey + "_copy";

                                                    // Check if _copy already exists
                                                    if (table[newKey] != nullptr) [[unlikely]]
                                                    {
                                                      continue;
                                                    }

                                                    // Copy the values
                                                    std::vector<Table::ValueType> values(row_size);
                                                    for (size_t i = 0; i < row_size; ++i) [[likely]]
                                                    {
                                                      values[i] = (*it)[i];
                                                    }

                                                    local_records.emplace_back(newKey,
                                                                               std::move(values));
                                                  }
                                                  return local_records;
                                                }));
    ++chunk_index;
  }

  // Merge results in order (preserve chunk order)
  std::vector<RecordPair> allRecords;
  for (auto& [idx, future] : tasks) [[likely]]
  {
    auto local_records = future.get();
    allRecords.insert(allRecords.end(), make_move_iterator(local_records.begin()),
                      make_move_iterator(local_records.end()));
  }

  return allRecords;
}
