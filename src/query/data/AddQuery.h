#ifndef PROJECT_ADDQUERY_H
#define PROJECT_ADDQUERY_H

#include <string>

#include "../Query.h"

class AddQuery : public ComplexQuery
{
  static constexpr const char* qname = "ADD";

public:
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute() override;

  std::string toString() override;
};

#endif
