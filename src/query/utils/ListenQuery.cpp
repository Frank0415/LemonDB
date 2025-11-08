#include "ListenQuery.h"

#include <cctype>
#include <exception>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>

#include "db/Database.h"
#include "query/QueryParser.h"
#include "query/QueryResult.h"
#include "query/management/CopyTableQuery.h"
#include "query/management/WaitQuery.h"
#include "threading/QueryManager.h"
#include "utils/formatter.h"



QueryResult::Ptr ListenQuery::execute()
{
  if (query_manager == nullptr || query_parser == nullptr || database == nullptr)
  {
    throw std::runtime_error("ListenQuery dependencies are not set");
  }

  std::ifstream infile(fileName);
  if (!infile.is_open())
  {
    return std::make_unique<ErrorMsgResult>(qname, "Cannot open file '?'"_f % fileName);
  }

  scheduled_query_count = 0;
  std::string raw_statement;

  try
  {
    while (readNextStatement(infile, raw_statement))
    {
      std::string trimmed = trimCopy(raw_statement);
      if (trimmed.empty())
      {
        continue;
      }
      if (trimmed.front() == '#')
      {
        continue;
      }

      try
      {
        Query::Ptr query = query_parser->parseQuery(trimmed);


  catch (const std::ios_base::failure&)
  {
    return std::make_unique<ErrorMsgResult>(qname, "Unexpected EOF in listen file '?'"_f % fileName);
  }

  return std::make_unique<ListenResult>(fileName);
}

std::string ListenQuery::toString()
{
  return "QUERY = Listen, FILE = \"" + fileName + "\"";
}
