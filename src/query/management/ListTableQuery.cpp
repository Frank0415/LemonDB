//
// Created by liu on 18-10-25.
//

#include "ListTableQuery.h"

#include <memory>
#include <string>

#include "../../db/Database.h"
#include "../QueryResult.h"

QueryResult::Ptr ListTableQuery::execute() {
  Database::getInstance().printAllTable();
  return std::make_unique<SuccessMsgResult>(qname);
}

std::string ListTableQuery::toString() { return "QUERY = LIST"; }
