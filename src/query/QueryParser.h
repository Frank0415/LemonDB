#ifndef SRC_QUERY_PARSER_H
#define SRC_QUERY_PARSER_H

#include <memory>
#include <string>
#include <vector>

#include "Query.h"

struct TokenizedQueryString {
  std::vector<std::string> token;
  std::string rawQeuryString;
};

class QueryBuilder {
public:
  QueryBuilder() = default;
  QueryBuilder(const QueryBuilder &) = delete;
  QueryBuilder &operator=(const QueryBuilder &) = delete;
  QueryBuilder(QueryBuilder &&) = default;
  QueryBuilder &operator=(QueryBuilder &&) = default;

  using Ptr = std::unique_ptr<QueryBuilder>;

  virtual Query::Ptr tryExtractQuery(TokenizedQueryString &queryString) = 0;
  virtual void setNext(Ptr &&builder) = 0;
  virtual void clear() = 0;

  virtual ~QueryBuilder() = default;
};

class QueryParser {
  QueryBuilder::Ptr first;       // An owning pointer
  QueryBuilder *last = nullptr;  // None owning reference

  static TokenizedQueryString
  tokenizeQueryString(const std::string &queryString);

public:
  QueryParser() = default;
  QueryParser(const QueryParser &) = delete;
  QueryParser &operator=(const QueryParser &) = delete;
  QueryParser(QueryParser &&) = default;
  QueryParser &operator=(QueryParser &&) = default;

  /**
   * Parse a query string into a Query object
   * @param queryString The query string to parse
   * @return Unique pointer to the parsed Query
   */
  Query::Ptr parseQuery(const std::string &queryString);

  /**
   * Register a query builder for parsing
   * @param qBuilder The query builder to register
   */
  void registerQueryBuilder(QueryBuilder::Ptr &&qBuilder);

  /**
   * Destruct a QueryParser
   */
  ~QueryParser() = default;
};

#endif  // SRC_QUERY_PARSER_H
