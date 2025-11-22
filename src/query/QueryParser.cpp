#include "QueryParser.h"

#include <sstream>
#include <string>
#include <utility>

#include "QueryBuilders.h"
#include "db/QueryBase.h"
#include "utils/uexception.h"

QueryParser::QueryParser() = default;

Query::Ptr QueryParser::parseQuery(const std::string &queryString) {
  if (first == nullptr) [[unlikely]] {
    throw QueryBuilderMatchFailed(queryString);
  }
  auto tokenized = tokenizeQueryString(queryString);
  if (tokenized.token.empty()) [[unlikely]] {
    throw QueryBuilderMatchFailed("");
  }
  first->clear();
  return first->tryExtractQuery(tokenized);
}

void QueryParser::registerQueryBuilder(QueryBuilder::Ptr &&qBuilder) {
  if (first == nullptr) [[unlikely]] {
    first = std::move(qBuilder);
    last = first.get();
    last->setNext(FailedQueryBuilder::getDefault());
  } else [[likely]] {
    QueryBuilder *temp = last;
    last = qBuilder.get();
    temp->setNext(std::move(qBuilder));
    last->setNext(FailedQueryBuilder::getDefault());
  }
}

TokenizedQueryString
QueryParser::tokenizeQueryString(const std::string &queryString) {
  TokenizedQueryString result;
  result.rawQeuryString = queryString;
  std::stringstream stream;
  stream.str(queryString);
  std::string tStr;
  while (stream >> tStr) [[likely]] {
    result.token.push_back(tStr);
  }
  return result;
}
