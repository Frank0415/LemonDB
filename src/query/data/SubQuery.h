#ifndef PROJECT_SUBQUERY_H
#define PROJECT_SUBQUERY_H

#include <string>

#include "../Query.h"

class SubQuery : public ComplexQuery
{
  static constexpr const char* qname = "SUB";

public:
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute() override;

  std::string toString() override;
};

#endif // PROJECT_SUBQUERY_H
