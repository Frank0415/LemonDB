#include "QueryBuilders.h"
#include "../db/Database.h"
#include "../utils/formatter.h"
#include "../utils/uexception.h"
#include "Query.h"
#include "QueryParser.h"
#include "data/AddQuery.h"
#include "data/CountQuery.h"
#include "data/DeleteQuery.h"
#include "data/DuplicateQuery.h"
#include "data/InsertQuery.h"
#include "data/MaxQuery.h"
#include "data/MinQuery.h"
#include "data/SelectQuery.h"
#include "data/SubQuery.h"
#include "data/SumQuery.h"
#include "data/SwapQuery.h"
#include "data/UpdateQuery.h"
#include "management/CopyTableQuery.h"
#include "management/DropTableQuery.h"
#include "management/DumpTableQuery.h"
#include "management/ListTableQuery.h"
#include "management/LoadTableQuery.h"
#include "management/PrintTableQuery.h"
#include "management/QuitQuery.h"
#include "management/TruncateTableQuery.h"
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>

namespace
{
void Throwhelper(std::vector<std::string>::const_iterator iterator,
                 std::vector<std::string>::const_iterator limit, const std::string&& message)
{
  if (iterator == limit)
  {
    throw IllFormedQuery(message);
  }
}
} // namespace

Query::Ptr FakeQueryBuilder::tryExtractQuery(TokenizedQueryString& query)
{
  std::cerr << "Query string: \n" << query.rawQeuryString << "\n";
  std::cerr << "Tokens:\n";
  constexpr int column_width = 10;
  constexpr int tokens_per_line = 5;
  int count = 0;
  for (const auto& tok : query.token)
  {
    std::cerr << std::setw(column_width) << "\"" << tok << "\"";
    count = (count + 1) % tokens_per_line;
    if (count == 4)
    {
      std::cerr << '\n';
    }
  }
  if (count != 4)
  {
    std::cerr << '\n';
  }
  return this->nextBuilder->tryExtractQuery(query);
}

Query::Ptr ManageTableQueryBuilder::tryExtractQuery(TokenizedQueryString& query)
{
  if (query.token.size() == 2)
  {
    if (query.token.front() == "LOAD")
    {
      auto& database = Database::getInstance();
      auto tableName = database.getFileTableName(query.token[1]);
      return std::make_unique<LoadTableQuery>(tableName, query.token[1]);
    }
    if (query.token.front() == "DROP")
    {
      return std::make_unique<DropTableQuery>(query.token[1]);
    }
    if (query.token.front() == "TRUNCATE")
    {
      return std::make_unique<TruncateTableQuery>(query.token[1]);
    }
  }
  if (query.token.size() == 3)
  {
    if (query.token.front() == "DUMP")
    {
      auto& database = Database::getInstance();
      database.updateFileTableName(query.token[2], query.token[1]);
      return std::make_unique<DumpTableQuery>(query.token[1], query.token[2]);
    }
    if (query.token.front() == "COPYTABLE")
    {
      return std::make_unique<CopyTableQuery>(query.token[1], query.token[2]);
    }
  }
  return this->nextBuilder->tryExtractQuery(query);
}

Query::Ptr DebugQueryBuilder::tryExtractQuery(TokenizedQueryString& query)
{
  if (query.token.size() == 1)
  {
    if (query.token.front() == "LIST")
    {
      return std::make_unique<ListTableQuery>();
    }
    if (query.token.front() == "QUIT")
    {
      return std::make_unique<QuitQuery>();
    }
  }
  if (query.token.size() == 2)
  {
    if (query.token.front() == "SHOWTABLE")
    {
      return std::make_unique<PrintTableQuery>(query.token[1]);
    }
  }
  return BasicQueryBuilder::tryExtractQuery(query);
}

void ComplexQueryBuilder::parseToken(TokenizedQueryString& query)
{
  // Treats forms like:
  //
  // $OPER$ ( arg1 arg2 ... )
  // FROM table
  // WHERE ( KEY = $STR$ ) ( $field$ $OP$ $int$ ) ...
  //
  // The "WHERE" clause can be ommitted
  // The args of OPER clause can be ommitted

  auto iterator = query.token.cbegin();
  auto end = query.token.cend();
  iterator += 1; // Take to args;
  Throwhelper(iterator, end, "Missing operands or FROM clause.");
  if (*iterator != "FROM")
  {
    if (*iterator != "(")
    {
      throw IllFormedQuery("Ill-formed operand.");
    }
    ++iterator;
    while (*iterator != ")")
    {
      this->operandToken.push_back(*iterator);
      ++iterator;
      Throwhelper(iterator, end, "Ill-formed operand.");
    }
    ++iterator;
    if (iterator == end || *iterator != "FROM")
    {
      throw IllFormedQuery("Missing FROM clause");
    }
  }
  if (++iterator == end)
  {
    throw IllFormedQuery("Missing target table");
  }
  this->targetTable = *iterator;
  if (++iterator == end) // the "WHERE" clause is ommitted
  {
    return;
  }
  if (*iterator != "WHERE")
  {
    // Hmmm, C++11 style Raw-string literal
    // http://en.cppreference.com/w/cpp/language/string_literal
    throw IllFormedQuery(R"(Expecting "WHERE", found "?".)"_f % *iterator);
  }
  while (++iterator != end)
  {
    if (*iterator != "(")
    {
      throw IllFormedQuery("Ill-formed query condition");
    }
    QueryCondition cond;
    cond.fieldId = 0;
    cond.valueParsed = 0;
    if (++iterator == end)
    {
      throw IllFormedQuery("Missing field in condition");
    }
    cond.field = *iterator;
    if (++iterator == end)
    {
      throw IllFormedQuery("Missing operator in condition");
    }
    cond.op = *iterator;
    if (++iterator == end)
    {
      throw IllFormedQuery("Missing  in condition");
    }
    cond.value = *iterator;
    ++iterator;
    if (iterator == end || *iterator != ")")
    {
      throw IllFormedQuery("Ill-formed query condition");
    }
    this->conditionToken.push_back(cond);
  }
}

Query::Ptr ComplexQueryBuilder::tryExtractQuery(TokenizedQueryString& query)
{
  try
  {
    this->parseToken(query);
  }
  catch (const IllFormedQuery& e)
  {
    std::cerr << e.what() << '\n';
    return this->nextBuilder->tryExtractQuery(query);
  }
  const std::string operation = query.token.front();
  if (operation == "INSERT")
  {
    return std::make_unique<InsertQuery>(this->targetTable, this->operandToken,
                                         this->conditionToken);
  }
  if (operation == "UPDATE")
  {
    return std::make_unique<UpdateQuery>(this->targetTable, this->operandToken,
                                         this->conditionToken);
  }
  if (operation == "SELECT")
  {
    return std::make_unique<SelectQuery>(this->targetTable, this->operandToken,
                                         this->conditionToken);
  }
  if (operation == "DELETE")
  {
    return std::make_unique<DeleteQuery>(this->targetTable, this->operandToken,
                                         this->conditionToken);
  }
  if (operation == "DUPLICATE")
  {
    return std::make_unique<DuplicateQuery>(this->targetTable, this->operandToken,
                                            this->conditionToken);
  }
  if (operation == "COUNT")
  {
    // return std::make_unique<NopQuery>(); // Not implemented
    // FIX: Enable CountQuery by creating an instance of it.
    // The empty operand list and the condition list are correctly handled.
    return std::make_unique<CountQuery>(this->targetTable, this->operandToken,
                                        this->conditionToken);
  }
  if (operation == "SUM")
  {
    return std::make_unique<SumQuery>(this->targetTable, this->operandToken, this->conditionToken);
  }
  if (operation == "MIN")
  {
    return std::make_unique<MinQuery>(this->targetTable, this->operandToken, this->conditionToken);
  }
  if (operation == "MAX")
  {
    return std::make_unique<MaxQuery>(this->targetTable, this->operandToken, this->conditionToken);
  }
  if (operation == "ADD")
  {
    return std::make_unique<AddQuery>(this->targetTable, this->operandToken, this->conditionToken);
  }
  if (operation == "SUB")
  {
    return std::make_unique<SubQuery>(this->targetTable, this->operandToken, this->conditionToken);
  }
  if (operation == "SWAP")
  {
    return std::make_unique<SwapQuery>(this->targetTable, this->operandToken, this->conditionToken);
  }
  std::cerr << "Complicated query found!" << '\n';
  std::cerr << "Operation = " << query.token.front() << '\n';
  std::cerr << "    Operands : ";
  for (const auto& oprand : this->operandToken)
  {
    std::cerr << oprand << " ";
  }
  std::cerr << '\n';
  std::cerr << "Target Table = " << this->targetTable << '\n';
  if (this->conditionToken.empty())
  {
    std::cerr << "No WHERE clause specified." << '\n';
  }
  else
  {
    std::cerr << "Conditions = ";
  }
  for (const auto& cond : this->conditionToken)
  {
    std::cerr << cond.field << cond.op << cond.value << " ";
  }
  std::cerr << '\n';

  return this->nextBuilder->tryExtractQuery(query);
}

void ComplexQueryBuilder::clear()
{
  this->conditionToken.clear();
  this->targetTable = "";
  this->operandToken.clear();
  this->nextBuilder->clear();
}
