#include "AddQuery.h"

#include <cstddef>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>

#include "../../db/Database.h"
#include "../../db/TableLockManager.h"
#include "../../utils/formatter.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"
#include "threading/Threadpool.h"

[[nodiscard]] QueryResult::Ptr AddQuery::execute()
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
    // this operands stores a list of ADD ( fields ... destField ) FROM table
    // WHERE ( cond ) ...;
    // No proper conditions implemented

    // Stategy: iterate through all rows that satisfy the condition and sum all
    // @ once
    if (!ThreadPool::isInitialized())
    {
      return executeSingleThreaded(table);
    }

    ThreadPool& pool = ThreadPool::getInstance();
    if (pool.getThreadCount() <= 1 || table.size() < Table::splitsize())
    {
      return executeSingleThreaded(table);
    }

    return executeMultiThreaded(table);
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

std::string AddQuery::toString()
{
  return "QUERY = ADD TABLE \"" + this->targetTable + "\"";
}

[[nodiscard]] QueryResult::Ptr AddQuery::validateOperands() const
{
  if (this->operands.size() < 2)
  {
    return std::make_unique<ErrorMsgResult>(
        qname, this->targetTable, "Invalid number of operands (? operands)."_f % operands.size());
  }
  return nullptr;
}

[[nodiscard]] QueryResult::Ptr AddQuery::executeSingleThreaded(Table& table)
{
  int count = 0;

  for (auto it = table.begin(); it != table.end(); ++it)
  {
    if (!this->evalCondition(*it))
    {
      continue;
    }
    // perform ADD operation
    int sum = 0;
    for (size_t i = 0; i < this->operands.size() - 1; ++i)
    {
      auto fieldIndex = table.getFieldIndex(this->operands[i]);
      sum += (*it)[fieldIndex];
    }
    (*it)[table.getFieldIndex(this->operands.back())] = sum;
    count++;
  }
  return std::make_unique<RecordCountResult>(count);
}

[[nodiscard]] QueryResult::Ptr AddQuery::executeMultiThreaded(Table& table)
{
  constexpr size_t CHUNK_SIZE = Table::splitsize();
  ThreadPool& pool = ThreadPool::getInstance();
  const size_t num_fields = this->operands.size() - 1;
  const size_t src_field_count = num_fields - 1;
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
  }
}