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

QueryResult::Ptr DropTableQuery::execute() {
  Database &database = Database::getInstance();
  try {
    auto lock =
        TableLockManager::getInstance().acquireWrite(this->targetTableRef());
    database.dropTable(this->targetTableRef());
    return std::make_unique<SuccessMsgResult>(qname);
  } catch (const TableNameNotFound &exc) {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            std::string("No such table."));
  } catch (const std::exception &exc) {
    return std::make_unique<ErrorMsgResult>(qname, exc.what());
  }
}

std::string DropTableQuery::toString() {
  return "QUERY = DROP, Table = \"" + targetTableRef() + "\"";
}
