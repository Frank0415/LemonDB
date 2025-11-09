#include "ListenQuery.h"

#include <cctype>
#include <cstddef>
#include <cstdio>
#include <exception>
#include <fstream>
#include <ios>
#include <iostream>
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

bool readNextStatement(std::istream& stream, std::string& out_statement)
{
  out_statement.clear();
  while (true)
  {
    const int character = stream.get();
    if (character == ';')
    {
      return true;
    }
    if (character == EOF)
    {
      if (out_statement.empty())
      {
        return false;
      }
      throw std::ios_base::failure("Unexpected end of input before ';'");
    }
    out_statement.push_back(static_cast<char>(character));
  }
}

void handleCopyTable(QueryManager& query_manager,
                     const std::string& trimmed,
                     const std::string& source_table,
                     CopyTableQuery* copy_query)
{
  if (copy_query == nullptr)
  {
    return;
  }

  constexpr size_t copytable_prefix_len = 9; // length of "COPYTABLE"
  std::string new_table_name = trimmed.substr(copytable_prefix_len);

  size_t position = new_table_name.find_first_not_of(" \t");
  if (position == std::string::npos)
  {
    return;
  }
  new_table_name = new_table_name.substr(position);

  position = new_table_name.find_first_of(" \t");
  if (position == std::string::npos)
  {
    return;
  }
  new_table_name = new_table_name.substr(position);

  position = new_table_name.find_first_not_of(" \t;");
  if (position == std::string::npos)
  {
    return;
  }
  new_table_name = new_table_name.substr(position);

  position = new_table_name.find_first_of(" \t;");
  if (position != std::string::npos)
  {
    new_table_name = new_table_name.substr(0, position);
  }

  auto wait_query = std::make_unique<WaitQuery>(source_table, copy_query->getWaitSemaphore());
  constexpr size_t wait_query_id = 0;
  query_manager.addQuery(wait_query_id, new_table_name, wait_query.release());
}
} // namespace

void ListenQuery::setDependencies(QueryManager* manager,
                                  QueryParser* parser,
                                  Database* database_ptr,
                                  std::atomic<size_t>* counter)
{
  query_manager = manager;
  query_parser = parser;
  database = database_ptr;
  query_counter = counter;
}

QueryResult::Ptr ListenQuery::execute()
{
  if (query_manager == nullptr || query_parser == nullptr || database == nullptr ||
      query_counter == nullptr)
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

        const size_t query_id = query_counter->fetch_add(1) + 1;
        query_manager->addQuery(query_id, query->targetTableRef(), query.release());
        scheduled_query_count++;
      }
      catch (const std::exception& /*ignored*/)
      {
        continue;
      }
    }
  }
  catch (const std::ios_base::failure&)
  {
    return std::make_unique<ErrorMsgResult>(qname,
                                            "Unexpected EOF in listen file '?'"_f % fileName);
  }

  return std::make_unique<ListenResult>(fileName);
}

std::string ListenQuery::toString()
{
  return "QUERY = Listen, FILE = \"" + fileName + "\"";
}
