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

QueryResult::Ptr SwapQuery::execute()
{
  if (this->operands.size() != 2)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable.c_str(),
                                            "Invalid number of operands (? operands)."_f %
                                                operands.size());
  }

  try
  {
    Database& database = Database::getInstance();
    auto lock = TableLockManager::getInstance().acquireWrite(this->targetTable);
    auto& table = database[this->targetTable];
    if (operands[0] == "KEY" || operands[1] == "KEY")
    {
      return std::make_unique<ErrorMsgResult>(qname, this->targetTable,
                                              "Ill-formed query: KEY cannot be swapped.");
    }
    const auto field_index_1 = table.getFieldIndex(operands[0]);
    const auto field_index_2 = table.getFieldIndex(operands[1]);

    Table::SizeType counter = 0;
    const bool handled = this->testKeyCondition(table,
                                                [&](bool success, Table::Object::Ptr&& obj)
                                                {
                                                  if (!success)
                                                  {
                                                    return;
                                                  }
                                                  if (obj)
                                                  {
                                                    auto tmp = (*obj)[field_index_1];
                                                    (*obj)[field_index_1] = (*obj)[field_index_2];
                                                    (*obj)[field_index_2] = tmp;
                                                    ++counter;
                                                  }
                                                });

    if (!handled)
    {
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
