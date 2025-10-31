//
// Created by liu on 18-10-25.
//

#include "DropTableQuery.h"

#include <exception>
#include <memory>
#include <string>

#include "../../db/Database.h"
#include "../../db/TableLockManager.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"

QueryResult::Ptr DropTableQuery::execute()
{
  using std::string_literals::operator""s;
  Database& database = Database::getInstance();
  try
  {
    auto lock = TableLockManager::getInstance().acquireWrite(this->targetTable);
    database.dropTable(this->targetTable);
    return std::make_unique<SuccessMsgResult>(qname);
  }
  catch (const TableNameNotFound& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, targetTable, "No such table."s);
  }
  catch (const std::exception& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, e.what());
  }
}

std::string DropTableQuery::toString()
{
  return "QUERY = DROP, Table = \"" + targetTable + "\"";
}
