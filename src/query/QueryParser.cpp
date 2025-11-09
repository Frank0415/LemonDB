#include "QueryParser.h"

#include <algorithm>
#include <cctype>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>

#include "QueryBuilders.h"
#include "db/QueryBase.h"
#include "utils/uexception.h"

QueryParser::QueryParser() = default;

Query::Ptr QueryParser::parseQuery(const std::string& queryString)
{
  if (first == nullptr) [[unlikely]]
  {
    throw QueryBuilderMatchFailed(queryString);
  }
  auto tokenized = tokenizeQueryString(queryString);
  if (tokenized.token.empty()) [[unlikely]]
  {
    throw QueryBuilderMatchFailed("");
  }
  first->clear();
  return first->tryExtractQuery(tokenized);
}

void QueryParser::registerQueryBuilder(QueryBuilder::Ptr&& qBuilder)
{
  if (first == nullptr) [[unlikely]]
  {
    first = std::move(qBuilder);
    last = first.get();
    last->setNext(FailedQueryBuilder::getDefault());
  }
  else [[likely]]
  {
    QueryBuilder* temp = last;
    last = qBuilder.get();
    temp->setNext(std::move(qBuilder));
    last->setNext(FailedQueryBuilder::getDefault());
  }
}

TokenizedQueryString QueryParser::tokenizeQueryString(const std::string& queryString)
{
  TokenizedQueryString result;
  result.rawQeuryString = queryString;
  result.token.reserve(16);
  
  std::string_view sv{queryString};
  auto is_space = [](char c) { return std::isspace(static_cast<unsigned char>(c)) != 0; };
  
  size_t start = 0;
  const size_t len = sv.size();
  
  while (start < len)
  {
    while (start < len && is_space(sv[start]))
    {
      ++start;
    }
    
    if (start >= len)
    {
      break;
    }
    
    size_t end = start;
    while (end < len && !is_space(sv[end]))
    {
      ++end;
    }
    
    result.token.emplace_back(sv.substr(start, end - start));
    start = end;
  }
  
  return result;
}
