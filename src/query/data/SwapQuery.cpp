#include "SwapQuery.h"

#include <exception>
#include <memory>
#include <stdexcept>
#include <string>

#include "../../db/Database.h"
#include "../../db/TableLockManager.h"
#include "../../utils/formatter.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"
#include "db/Table.h"
#include "threading/Threadpool.h"

QueryResult::Ptr SwapQuery::execute()
{

  try
  {
    auto validation_result = validateOperands();
    if (validation_result != nullptr)
    {
      return validation_result;
    }

    auto lock = TableLockManager::getInstance().acquireWrite(this->targetTable);
    Database& database = Database::getInstance();
    auto& table = database[this->targetTable];

    const auto fids = getFieldIndices(table);
    auto field_index_1 = fids.first;
    auto field_index_2 = fids.second;

    if (!ThreadPool::isInitialized())
    {
      return executeSingleThreaded(table, field_index_1, field_index_2);
    }

    ThreadPool& pool = ThreadPool::getInstance();
    if (pool.getThreadCount() <= 1 || table.size() < Table::splitsize())
    {
      return executeSingleThreaded(table, field_index_1, field_index_2);
    }

    return executeMultiThreaded(table, field_index_1, field_index_2);

    Table::SizeType counter = 0;

    for (auto it = table.begin(); it != table.end(); ++it)
    {
      if (this->evalCondition(*it))
      {
        auto tmp = (*it)[field_index_1];
        (*it)[field_index_1] = (*it)[field_index_2];
        (*it)[field_index_2] = tmp;
        ++counter;
      }
    }
    return std::make_unique<RecordCountResult>(static_cast<int>(counter));
  }
  catch (const TableNameNotFound&)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable, "No such table.");
  }
  catch (const IllFormedQueryCondition& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
  }
  catch (const std::invalid_argument& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable,
                                            "Unknown error '?'"_f % e.what());
  }
  catch (const std::exception& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable,
                                            "Unkonwn error '?'."_f % e.what());
  }
}
std::string SwapQuery::toString()
{
  return "QUERY = SWAP \"" + this->targetTable + "\"";
}

[[nodiscard]] QueryResult::Ptr SwapQuery::validateOperands() const
{
  if (this->operands.size() != 2)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable.c_str(),
                                            "Invalid number of operands (? operands)."_f %
                                                operands.size());
  }
  return nullptr;
}

[[nodiscard]] std::pair<const Table::FieldIndex, const Table::FieldIndex>
SwapQuery::getFieldIndices(Table& table) const
{
  if (operands[0] == "KEY" || operands[1] == "KEY")
  {
    throw std::make_unique<ErrorMsgResult>(qname, this->targetTable,
                                           "Ill-formed query: KEY cannot be swapped.");
  }
  return {table.getFieldIndex(operands[0]), table.getFieldIndex(operands[1])};
}

[[nodiscard]] QueryResult::Ptr executeSingleThreaded(Table& table,
                                                     const Table::FieldIndex& field_index_1,
                                                     const Table::FieldIndex& field_index_2)
{
}

[[nodiscard]] QueryResult::Ptr executeMultiThreaded(Table& table,
                                                    const Table::FieldIndex& field_index_1,
                                                    const Table::FieldIndex& field_index_2)
{
}