#include "QueryParser.h"

#include <cctype>
#include <cstddef>
#include <string>
#include <string_view>
#include <utility>

#include "QueryBuilders.h"
#include "db/QueryBase.h"
#include "utils/uexception.h"

namespace {
constexpr size_t kDefaultTokenReserve = 16;
}

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
  result.token.reserve(kDefaultTokenReserve);

  const std::string_view view{queryString};
  auto is_space = [](char character) {
    return std::isspace(static_cast<unsigned char>(character)) != 0;
  };

  size_t start = 0;
  const size_t len = view.size();

  while (start < len) {
    while (start < len && is_space(view[start])) {
      ++start;
    }

    if (start >= len) {
      break;
    }

    size_t end = start;
    while (end < len && !is_space(view[end])) {
      ++end;
    }

    result.token.emplace_back(view.substr(start, end - start));
    start = end;
  }

  return result;
}
