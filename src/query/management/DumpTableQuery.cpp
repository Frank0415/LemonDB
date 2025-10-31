//
// Created by liu on 18-10-25.
//

#include "DumpTableQuery.h"

#include <exception>
#include <fstream>
#include <memory>
#include <string>

#include "../../db/Database.h"
#include "../../db/TableLockManager.h"
#include "../../utils/formatter.h"
#include "../QueryResult.h"

QueryResult::Ptr DumpTableQuery::execute()
{
  const auto& database = Database::getInstance();
  try
  {
    auto lock = TableLockManager::getInstance().acquireRead(this->targetTable);
    std::ofstream outfile(this->fileName);
    if (!outfile.is_open())
    {
      return std::make_unique<ErrorMsgResult>(qname, "Cannot open file '?'"_f % this->fileName);
    }
    outfile << database[this->targetTable];
    outfile.close();
    return std::make_unique<SuccessMsgResult>(qname, targetTable);
  }
  catch (const std::exception& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, e.what());
  }
}

std::string DumpTableQuery::toString()
{
  return "QUERY = Dump TABLE, FILE = \"" + this->fileName + "\"";
}
