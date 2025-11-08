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

namespace
{
std::string trimCopy(std::string_view input)
{
  const auto first = input.find_first_not_of(" \t\n\r");
  if (first == std::string_view::npos)
  {
    return "";
  }
  const auto last = input.find_last_not_of(" \t\n\r");
  return std::string(input.substr(first, last - first + 1));
}

bool startsWithCaseInsensitive(std::string_view value, std::string_view prefix)
{
  if (value.size() < prefix.size())
  {
    return false;
  }
  for (size_t index = 0; index < prefix.size(); ++index)
  {
    const auto lhs = static_cast<unsigned char>(value[index]);
    const auto rhs = static_cast<unsigned char>(prefix[index]);
    if (std::toupper(lhs) != std::toupper(rhs))
    {
      return false;
    }
  }
  return true;
}


} // namespace

void ListenQuery::setDependencies(QueryManager* manager, QueryParser* parser, Database* database_ptr)
{
  query_manager = manager;
  query_parser = parser;
  database = database_ptr;
}

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

        if (startsWithCaseInsensitive(trimmed, "QUIT"))
        {
          database->exit();
          break;
        }

        if (startsWithCaseInsensitive(trimmed, "COPYTABLE"))
        {
          handleCopyTable(*query_manager, trimmed, query->targetTableRef(),
                          dynamic_cast<CopyTableQuery*>(query.get()));
        }

        const size_t query_id = ++scheduled_query_count;
        query_manager->addQuery(query_id, query->targetTableRef(), query.release());
      }
      catch (const std::exception& /*ignored*/)
      {
        continue;
      }
    }
  }
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
