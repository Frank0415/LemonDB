//
// Created by liu on 18-10-25.
//

#include "PrintTableQuery.h"

#include <iostream>
#include <memory>
#include <string>

#include "../../db/Database.h"
#include "../../db/TableLockManager.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"

QueryResult::Ptr PrintTableQuery::execute()
{
  using std::string_literals::operator""s;
  const Database& database = Database::getInstance();
  try
  {
    auto lock = TableLockManager::getInstance().acquireRead(this->targetTable);
    const auto& table = database[this->targetTable];
    std::cout << "================\n";
    std::cout << "TABLE = ";
    std::cout << table;
    std::cout << "================\n" << '\n';
    return std::make_unique<SuccessMsgResult>(qname, this->targetTable);
  }
  catch (const TableNameNotFound& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable, "No such table."s);
  }
}

std::string PrintTableQuery::toString()
{
  return "QUERY = SHOWTABLE, Table = \"" + this->targetTable + "\"";
}
