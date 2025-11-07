#include "TruncateTableQuery.h"

#include <exception>
#include <memory>
#include <string>

#include "db/Database.h"
#include "db/TableLockManager.h"
#include "utils/uexception.h"
#include "query/QueryResult.h"

constexpr const char* qname_tr = "TRUNCATE";

QueryResult::Ptr TruncateTableQuery::execute()
{
  try
  {
    auto& database = Database::getInstance();
    auto lock = TableLockManager::getInstance().acquireWrite(this->targetTableRef());
    auto& table = database[this->targetTableRef()];
    table.clear();

    return std::make_unique<NullQueryResult>(); // silent success
  }
  catch (const TableNameNotFound&)
  {
    return std::make_unique<ErrorMsgResult>(qname_tr, this->targetTableRef(), "No such table.");
  }
  catch (const std::exception& exc)
  {
    return std::make_unique<ErrorMsgResult>(qname_tr, this->targetTableRef(), "Unknown error");
  }
}

std::string TruncateTableQuery::toString()
{
  return "QUERY = TRUNCATE \"" + this->targetTableRef() + "\"";
}
