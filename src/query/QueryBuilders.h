//
// Created by liu on 18-10-25.
//

#ifndef PROJECT_QUERYBUILDERS_H
#define PROJECT_QUERYBUILDERS_H

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "Query.h"
#include "QueryParser.h"

class FailedQueryBuilder : public QueryBuilder
{
public:
  /**
   * Get the default failed query builder instance
   * @return Unique pointer to FailedQueryBuilder
   */
  static QueryBuilder::Ptr getDefault()
  {
    return std::make_unique<FailedQueryBuilder>();
  }

  /**
   * Always throws QueryBuilderMatchFailed exception
   * @param query The tokenized query string
   * @return Never returns, always throws
   */
  Query::Ptr tryExtractQuery(TokenizedQueryString& query) final
  {
    throw QueryBuilderMatchFailed(query.rawQeuryString);
  }

  /**
   * Ignores the next builder (does nothing)
   * @param builder The next builder to set (ignored)
   */
  void setNext(QueryBuilder::Ptr&& builder) final
  {
    (void)std::move(builder);
  }

  /**
   * Clear any state (no-op for this builder)
   */
  void clear() override
  {
  }

  ~FailedQueryBuilder() override = default;
};

class BasicQueryBuilder : public QueryBuilder
{
private:
  QueryBuilder::Ptr nextBuilder;

public:
  /**
   * Get reference to the next builder in the chain
   * @return Reference to the next builder pointer
   */
  [[nodiscard]] QueryBuilder::Ptr& getNextBuilder()
  {
    return nextBuilder;
  }

  /**
   * Set the next builder in the chain
   * @param builder The next builder to set
   */
  void setNext(Ptr&& builder) override
  {
    nextBuilder = std::move(builder);
  }

  /**
   * Try to extract query using the next builder in chain
   * @param query The tokenized query string
   * @return Query pointer from next builder
   */
  Query::Ptr tryExtractQuery(TokenizedQueryString& query) override
  {
    return nextBuilder->tryExtractQuery(query);
  }

  /**
   * Constructor - initializes with default failed builder
   */
  BasicQueryBuilder() : nextBuilder(FailedQueryBuilder::getDefault())
  {
  }

  /**
   * Clear the next builder's state
   */
  void clear() override
  {
    nextBuilder->clear();
  }

  ~BasicQueryBuilder() override = default;
};

class ComplexQueryBuilder : public BasicQueryBuilder
{
private:
  std::string targetTable;
  std::vector<std::string> operandToken;
  std::vector<QueryCondition> conditionToken;

  /**
   * Parse tokens from the query string (virtual method for subclasses)
   * @param query The tokenized query string to parse
   */
  virtual void parseToken(TokenizedQueryString& query);

public:
  /**
   * Clear parsed tokens and target table
   */
  void clear() override;

  /**
   * Try to extract and build a complex query from tokens
   * @param query The tokenized query string
   * @return Query pointer if successful, nullptr otherwise
   */
  Query::Ptr tryExtractQuery(TokenizedQueryString& query) override;
};

// Transparant builder
// It does not modify or extract anything
// It prints current tokenized string
// Use to examine the queries and tokenizer
class FakeQueryBuilder : public BasicQueryBuilder
{
  /**
   * Pass query to next builder without modification (debugging aid)
   * @param query The tokenized query string
   * @return Query pointer from next builder
   */
  Query::Ptr tryExtractQuery(TokenizedQueryString& query) override;
};

inline Query::Ptr FakeQueryBuilder::tryExtractQuery(TokenizedQueryString& query)
{
  //   std::cerr << "Query string: \n" << query.rawQeuryString << "\n";
  //   std::cerr << "Tokens:\n";
  //   constexpr int column_width = 10;
  //   constexpr int tokens_per_line = 5;
  //   int count = 0;
  //   for (const auto& tok : query.token)
  //   {
  //     std::cerr << std::setw(column_width) << "\"" << tok << "\"";
  //     count = (count + 1) % tokens_per_line;
  //     if (count == 4)
  //     {
  //       std::cerr << '\n';
  //     }
  //   }
  //   if (count != 4)
  //   {
  //     std::cerr << '\n';
  //   }
  return getNextBuilder()->tryExtractQuery(query);
}

// Debug commands / Utils
class DebugQueryBuilder : public BasicQueryBuilder
{
  /**
   * Handle debug/utility queries
   * @param query The tokenized query string
   * @return Query pointer for debug commands
   */
  Query::Ptr tryExtractQuery(TokenizedQueryString& query) override;
};

// Load, dump, truncate and delete table
class ManageTableQueryBuilder : public BasicQueryBuilder
{
  /**
   * Handle table management queries (LOAD, DUMP, TRUNCATE, DROP)
   * @param query The tokenized query string
   * @return Query pointer for table management operations
   */
  Query::Ptr tryExtractQuery(TokenizedQueryString& query) override;
};

// ComplexQueryBuilderClass(UpdateTable);
// ComplexQueryBuilderClass(Insert);
// ComplexQueryBuilderClass(Delete);
// ComplexQueryBuilderClass(Count);

#endif // PROJECT_QUERYBUILDERS_H
