//
// Created by liu on 18-10-25.
//

#include "DumpTableQuery.h"

#include <exception>
#include <fstream>
#include <memory>
#include <string>

#include "db/Database.h"
#include "db/TableLockManager.h"
#include "utils/formatter.h"
#include "query/QueryResult.h"

QueryResult::Ptr DumpTableQuery::execute()
{
  const auto& database = Database::getInstance();
  try
  {
    auto lock = TableLockManager::getInstance().acquireRead(this->targetTableRef());
    std::ofstream outfile(this->fileName);
    if (!outfile.is_open()) [[unlikely]]
    {
      return std::make_unique<ErrorMsgResult>(qname, "Cannot open file '?'"_f % this->fileName);
    }
    outfile << database[this->targetTableRef()];
    outfile.close();
    return std::make_unique<SuccessMsgResult>(qname, this->targetTableRef());
  }
  catch (const std::exception& exc)
  {
    return std::make_unique<ErrorMsgResult>(qname, exc.what());
  }
}

std::string DumpTableQuery::toString()
{
  return "QUERY = Dump TABLE, FILE = \"" + this->fileName + "\"";
}
