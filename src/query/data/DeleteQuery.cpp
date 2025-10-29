#include "DeleteQuery.h"

#include <exception>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "../../db/Database.h"
#include "../../db/Table.h"
#include "../../utils/formatter.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"

QueryResult::Ptr DeleteQuery::execute()
{
  using std::string_literals::operator""s;
  if (!this->operands.empty())
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable.c_str(),
                                            "Invalid number of operands (? operands)."_f %
                                                operands.size());
  }

  try
  {
    Database& database = Database::getInstance();
    Table::SizeType counter = 0;
    auto& table = database[this->targetTable];
    auto result = initCondition(table);
    if (result.second)
    {
      std::vector<Table::KeyType> keysToDelete;
      for (auto it = table.begin(); it != table.end(); it++)
      {
        if (this->evalCondition(*it))
        {
          keysToDelete.push_back(it->key());
          ++counter;
        }
      }
      for (const auto& key : keysToDelete)
      {
        table.deleteByIndex(key);
      }
    }
    else
    {
      throw IllFormedQueryCondition("Error conditions in WHERE clause.");
    }
    return std::make_unique<RecordCountResult>(counter);
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

std::string DeleteQuery::toString()
{
  return "QUERY = DELETE " + this->targetTable + "\"";
}
