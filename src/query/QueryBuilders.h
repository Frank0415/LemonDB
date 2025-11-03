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
  static QueryBuilder::Ptr getDefault()
  {
    return std::make_unique<FailedQueryBuilder>();
  }

  Query::Ptr tryExtractQuery(TokenizedQueryString& query) final
  {
    throw QueryBuilderMatchFailed(query.rawQeuryString);
  }

  void setNext(QueryBuilder::Ptr&& builder) final
  {
    (void)std::move(builder);
  }

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
  // Helper method for derived classes to access nextBuilder
  [[nodiscard]] QueryBuilder::Ptr& getNextBuilder()
  {
    return nextBuilder;
  }

  void setNext(Ptr&& builder) override
  {
    nextBuilder = std::move(builder);
  }

  Query::Ptr tryExtractQuery(TokenizedQueryString& query) override
  {
    return nextBuilder->tryExtractQuery(query);
  }

  BasicQueryBuilder() : nextBuilder(FailedQueryBuilder::getDefault())
  {
  }

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

  virtual void parseToken(TokenizedQueryString& query);

public:
  void clear() override;

  // Used as a debugging function.
  // Prints the parsed information
  Query::Ptr tryExtractQuery(TokenizedQueryString& query) override;
};

// Transparant builder
// It does not modify or extract anything
// It prints current tokenized string
// Use to examine the queries and tokenizer
class FakeQueryBuilder : public BasicQueryBuilder
{
  Query::Ptr tryExtractQuery(TokenizedQueryString& query) override;
};

// Debug commands / Utils
class DebugQueryBuilder : public BasicQueryBuilder
{
  Query::Ptr tryExtractQuery(TokenizedQueryString& query) override;
};

// Load, dump, truncate and delete table
class ManageTableQueryBuilder : public BasicQueryBuilder
{
  Query::Ptr tryExtractQuery(TokenizedQueryString& query) override;
};

// ComplexQueryBuilderClass(UpdateTable);
// ComplexQueryBuilderClass(Insert);
// ComplexQueryBuilderClass(Delete);
// ComplexQueryBuilderClass(Count);

#endif // PROJECT_QUERYBUILDERS_H
