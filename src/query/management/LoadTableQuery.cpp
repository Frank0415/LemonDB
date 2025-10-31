//
// Created by liu on 18-10-25.
//

#include "LoadTableQuery.h"

#include <exception>
#include <fstream>
#include <memory>
#include <string>

#include "../../db/Database.h"
#include "../../db/TableLockManager.h"
#include "../../utils/formatter.h"
#include "../QueryResult.h"

QueryResult::Ptr LoadTableQuery::execute()
{
  try
  {
    // LOAD creates a new table, so we acquire write lock for the new table name
    auto lock = TableLockManager::getInstance().acquireWrite(this->targetTable);
    std::ifstream infile(this->fileName);
    if (!infile.is_open())
    {
      return std::make_unique<ErrorMsgResult>(qname, "Cannot open file '?'"_f % this->fileName);
    }
    Database::loadTableFromStream(infile, this->fileName);
    infile.close();
    return std::make_unique<SuccessMsgResult>(qname, targetTable);
  }
  catch (const std::exception& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, e.what());
  }
}

std::string LoadTableQuery::toString()
{
  return "QUERY = Load TABLE, FILE = \"" + this->fileName + "\"";
}
