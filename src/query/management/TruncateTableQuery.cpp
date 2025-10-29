#include "TruncateTableQuery.h"

#include <exception>
#include <memory>
#include <string>

#include "../../db/Database.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"

constexpr const char* qname_tr = "TRUNCATE";

QueryResult::Ptr TruncateTableQuery::execute()
{
  try
  {
    auto& db = Database::getInstance();
    auto& table = db[this->targetTable];
    table.clear();

    return std::make_unique<NullQueryResult>(); // silent success
  }
  catch (const TableNameNotFound&)
  {
    return std::make_unique<ErrorMsgResult>(qname_tr, this->targetTable, "No such table.");
  }
  catch (const std::exception& e)
  {
    return std::make_unique<ErrorMsgResult>(qname_tr, this->targetTable, "Unknown error");
  }
}

std::string TruncateTableQuery::toString()
{
  return "QUERY = TRUNCATE \"" + this->targetTable + "\"";
}
