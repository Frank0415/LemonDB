
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "../../db/Database.h"
#include "../../db/Table.h"
#include "../../db/TableLockManager.h"
#include "../../utils/formatter.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"
#include "DeleteQuery.h"

QueryResult::Ptr DeleteQuery::execute()
{
  if (!this->getOperands().empty()) [[unlikely]]
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef().c_str(),
                                            "Invalid number of operands (? operands)."_f %
                                                getOperands().size());
  }

  try
  {
    Database& database = Database::getInstance();
    auto lock = TableLockManager::getInstance().acquireWrite(this->targetTableRef());
    Table::SizeType counter = 0;
    auto& table = database[this->targetTableRef()];
    auto result = initCondition(table);
    if (result.second) [[likely]]
    {
      std::vector<Table::KeyType> keysToDelete;
      for (auto it = table.begin(); it != table.end(); it++) [[likely]]
      {
        if (this->evalCondition(*it)) [[likely]]
        {
          keysToDelete.push_back(it->key());
          ++counter;
        }
      }
      for (const auto& key : keysToDelete) [[likely]]
      {
        table.deleteByIndex(key);
      }
    }
    else [[unlikely]]
    {
      throw IllFormedQueryCondition("Error conditions in WHERE clause.");
    }
    return std::make_unique<RecordCountResult>(counter);
  }
  catch (const NotFoundKey& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(), "Key not found.");
  }
  catch (const TableNameNotFound& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(), "No such table.");
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

std::string DeleteQuery::toString()
{
  return "QUERY = DELETE " + this->targetTableRef() + "\"";
}
