#include "QueryBuilders.h"
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
#include "../db/Database.h"
#include "../db/QueryBase.h"
#include "management/CopyTableQuery.h"
#include "management/DropTableQuery.h"
#include "management/DumpTableQuery.h"
#include "management/ListTableQuery.h"
#include "management/LoadTableQuery.h"
#include "management/PrintTableQuery.h"
#include "management/QuitQuery.h"
#include "management/TruncateTableQuery.h"
#include "utils/ListenQuery.h"
#include "../utils/formatter.h"
#include "../utils/uexception.h"
#include "Query.h"
#include "QueryParser.h"

#include <memory>
#include <string>
#include <vector>

namespace {
void Throwhelper(std::vector<std::string>::const_iterator iterator,
                 std::vector<std::string>::const_iterator limit,
                 const std::string &&message) {
  if (iterator == limit) [[unlikely]] {
    throw IllFormedQuery(message);
  }
}

std::string ExtractListenFilename(const TokenizedQueryString &query) {
  std::string filename;

  if (query.token.size() >= 3 && query.token[1] == "(") {
    filename = query.token[2];
  } else if (query.token.size() >= 2) {
    filename = query.token[1];
  }

  if (!filename.empty() && filename.back() == ')') {
    filename.pop_back();
  }

  return filename;
}
}  // namespace

Query::Ptr
ManageTableQueryBuilder::tryExtractQuery(TokenizedQueryString &query) {
  if (query.token.size() >= 2) [[likely]] {
    if (query.token.front() == "LISTEN") [[unlikely]] {
      // Handle: LISTEN ( filename ) or LISTEN filename
      return std::make_unique<ListenQuery>(ExtractListenFilename(query));
    }
    if (query.token.front() == "LOAD") [[unlikely]] {
      auto &database = Database::getInstance();
      auto tableName = database.getFileTableName(query.token[1]);
      return std::make_unique<LoadTableQuery>(tableName, query.token[1]);
    }
    if (query.token.front() == "DROP") [[unlikely]] {
      return std::make_unique<DropTableQuery>(query.token[1]);
    }
    if (query.token.front() == "TRUNCATE") [[unlikely]] {
      return std::make_unique<TruncateTableQuery>(query.token[1]);
    }
  }
  if (query.token.size() == 3) [[unlikely]] {
    if (query.token.front() == "DUMP") [[unlikely]] {
      auto &database = Database::getInstance();
      database.updateFileTableName(query.token[2], query.token[1]);
      return std::make_unique<DumpTableQuery>(query.token[1], query.token[2]);
    }
    if (query.token.front() == "COPYTABLE") [[unlikely]] {
      return std::make_unique<CopyTableQuery>(query.token[1], query.token[2]);
    }
  }
  return getNextBuilder()->tryExtractQuery(query);
}

Query::Ptr DebugQueryBuilder::tryExtractQuery(TokenizedQueryString &query) {
  if (query.token.size() == 1) [[unlikely]] {
    if (query.token.front() == "LIST") [[unlikely]] {
      return std::make_unique<ListTableQuery>();
    }
    if (query.token.front() == "QUIT") [[unlikely]] {
      return std::make_unique<QuitQuery>();
    }
  }
  if (query.token.size() == 2) [[unlikely]] {
    if (query.token.front() == "SHOWTABLE") [[unlikely]] {
      return std::make_unique<PrintTableQuery>(query.token[1]);
    }
  }
  return BasicQueryBuilder::tryExtractQuery(query);
}

void ComplexQueryBuilder::parseToken(TokenizedQueryString &query) {
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
  iterator += 1;  // Take to args;
  Throwhelper(iterator, end, "Missing operands or FROM clause.");
  if (*iterator != "FROM") [[likely]] {
    if (*iterator != "(") [[unlikely]] {
      throw IllFormedQuery("Ill-formed operand.");
    }
    ++iterator;
    while (*iterator != ")") [[likely]] {
      this->operandToken.push_back(*iterator);
      ++iterator;
      Throwhelper(iterator, end, "Ill-formed operand.");
    }
    ++iterator;
    if (iterator == end || *iterator != "FROM") [[unlikely]] {
      throw IllFormedQuery("Missing FROM clause");
    }
  }
  if (++iterator == end) [[unlikely]] {
    throw IllFormedQuery("Missing target table");
  }
  this->targetTable = *iterator;
  if (++iterator == end) [[unlikely]]  // the "WHERE" clause is ommitted
  {
    return;
  }
  if (*iterator != "WHERE") [[unlikely]] {
    // Hmmm, C++11 style Raw-string literal
    // http://en.cppreference.com/w/cpp/language/string_literal
    throw IllFormedQuery(R"(Expecting "WHERE", found "?".)"_f % *iterator);
  }
  while (++iterator != end) [[likely]] {
    if (*iterator != "(") [[unlikely]] {
      throw IllFormedQuery("Ill-formed query condition");
    }
    QueryCondition cond;
    cond.fieldId = 0;
    cond.valueParsed = 0;
    if (++iterator == end) [[unlikely]] {
      throw IllFormedQuery("Missing field in condition");
    }
    cond.field = *iterator;
    if (++iterator == end) [[unlikely]] {
      throw IllFormedQuery("Missing operator in condition");
    }
    cond.op = *iterator;
    if (++iterator == end) [[unlikely]] {
      throw IllFormedQuery("Missing  in condition");
    }
    cond.value = *iterator;
    ++iterator;
    if (iterator == end || *iterator != ")") [[unlikely]] {
      throw IllFormedQuery("Ill-formed query condition");
    }
    this->conditionToken.push_back(cond);
  }
}

Query::Ptr ComplexQueryBuilder::tryExtractQuery(TokenizedQueryString &query) {
  try {
    this->parseToken(query);
  } catch (const IllFormedQuery &e) {
    // std::cerr << e.what() << '\n';
    return getNextBuilder()->tryExtractQuery(query);
  }
  const std::string operation = query.token.front();
  if (operation == "INSERT") [[likely]] {
    return std::make_unique<InsertQuery>(this->targetTable, this->operandToken,
                                         this->conditionToken);
  }
  if (operation == "UPDATE") [[likely]] {
    return std::make_unique<UpdateQuery>(this->targetTable, this->operandToken,
                                         this->conditionToken);
  }
  if (operation == "SELECT") [[likely]] {
    return std::make_unique<SelectQuery>(this->targetTable, this->operandToken,
                                         this->conditionToken);
  }
  if (operation == "DELETE") [[likely]] {
    return std::make_unique<DeleteQuery>(this->targetTable, this->operandToken,
                                         this->conditionToken);
  }
  if (operation == "DUPLICATE") [[likely]] {
    return std::make_unique<DuplicateQuery>(
        this->targetTable, this->operandToken, this->conditionToken);
  }
  if (operation == "COUNT") [[likely]] {
    return std::make_unique<CountQuery>(this->targetTable, this->operandToken,
                                        this->conditionToken);
  }
  if (operation == "SUM") [[likely]] {
    return std::make_unique<SumQuery>(this->targetTable, this->operandToken,
                                      this->conditionToken);
  }
  if (operation == "MIN") [[likely]] {
    return std::make_unique<MinQuery>(this->targetTable, this->operandToken,
                                      this->conditionToken);
  }
  if (operation == "MAX") [[likely]] {
    return std::make_unique<MaxQuery>(this->targetTable, this->operandToken,
                                      this->conditionToken);
  }
  if (operation == "ADD") [[likely]] {
    return std::make_unique<AddQuery>(this->targetTable, this->operandToken,
                                      this->conditionToken);
  }
  if (operation == "SUB") [[likely]] {
    return std::make_unique<SubQuery>(this->targetTable, this->operandToken,
                                      this->conditionToken);
  }
  if (operation == "SWAP") [[likely]] {
    return std::make_unique<SwapQuery>(this->targetTable, this->operandToken,
                                       this->conditionToken);
  }
  //   std::cerr << "Complicated query found!" << '\n';
  //   std::cerr << "Operation = " << query.token.front() << '\n';
  //   std::cerr << "    Operands : ";
  //   for (const auto& oprand : this->operandToken) [[likely]]
  //   {
  //     std::cerr << oprand << " ";
  //   }
  //   std::cerr << '\n';
  //   std::cerr << "Target Table = " << this->targetTable << '\n';
  //   if (this->conditionToken.empty()) [[unlikely]]
  //   {
  //     std::cerr << "No WHERE clause specified." << '\n';
  //   }
  //   else [[likely]]
  //   {
  //     std::cerr << "Conditions = ";
  //   }
  //   for (const auto& cond : this->conditionToken) [[likely]]
  //   {
  //     std::cerr << cond.field << cond.op << cond.value << " ";
  //   }
  //   std::cerr << '\n';

  return getNextBuilder()->tryExtractQuery(query);
}

void ComplexQueryBuilder::clear() {
  this->conditionToken.clear();
  this->targetTable = "";
  this->operandToken.clear();
  getNextBuilder()->clear();
}
