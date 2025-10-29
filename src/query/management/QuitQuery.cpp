//
// Created by liu on 18-10-25.
//

#include "QuitQuery.h"

#include <memory>
#include <string>

#include "../../db/Database.h"
#include "../QueryResult.h"

std::string QuitQuery::toString()
{
  return "QUERY = Quit";
}

QueryResult::Ptr QuitQuery::execute()
{
  auto& database = Database::getInstance();
  database.exit();
  // might not reach here, but we want to keep the consistency of queries
  return std::make_unique<SuccessMsgResult>(qname);
}
