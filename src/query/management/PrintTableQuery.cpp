//
// Created by liu on 18-10-25.
//

#include "PrintTableQuery.h"

#include <iostream>
#include <memory>
#include <string>

#include "db/Database.h"
#include "db/TableLockManager.h"
#include "query/QueryResult.h"
#include "utils/uexception.h"

QueryResult::Ptr PrintTableQuery::execute()
{
  const Database& database = Database::getInstance();
  try
  {
    auto lock = TableLockManager::getInstance().acquireRead(this->targetTableRef());
    const auto& table = database[this->targetTableRef()];
    std::cout << "================\n";
    std::cout << "TABLE = ";
    std::cout << table;
    std::cout << "================\n" << '\n';
    return std::make_unique<SuccessMsgResult>(qname, this->targetTableRef());
  }
  catch (const TableNameNotFound& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            std::string("No such table."));
  }
}

std::string PrintTableQuery::toString()
{
  return "QUERY = SHOWTABLE, Table = \"" + this->targetTableRef() + "\"";
}
