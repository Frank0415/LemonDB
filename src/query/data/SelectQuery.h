#ifndef PROJECT_SELECT_QUERY_H
#define PROJECT_SELECT_QUERY_H
#include <string>

#include "../Query.h"
#include "../QueryResult.h"

class SelectQuery : public ComplexQuery
{
  static constexpr const char* qname = "SELECT";

public:
  using ComplexQuery::ComplexQuery;
  QueryResult::Ptr execute() override;
  std::string toString() override;
};

#endif // PROJECT_SELECT_QUERY_H
