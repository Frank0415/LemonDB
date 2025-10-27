//
// Created by liu on 18-10-25.
//

#include "PrintTableQuery.h"

#include <iostream>
#include <memory>
#include <string>

#include "../../db/Database.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"

QueryResult::Ptr PrintTableQuery::execute()
{
  using namespace std;
  const Database& db = Database::getInstance();
  try
  {
    const auto& table = db[this->targetTable];
    cout << "================\n";
    cout << "TABLE = ";
    cout << table;
    cout << "================\n" << '\n';
    return make_unique<SuccessMsgResult>(qname, this->targetTable);
  }
  catch (const TableNameNotFound& e)
  {
    return make_unique<ErrorMsgResult>(qname, this->targetTable, "No such table."s);
  }
}

std::string PrintTableQuery::toString()
{
  return "QUERY = SHOWTABLE, Table = \"" + this->targetTable + "\"";
}
