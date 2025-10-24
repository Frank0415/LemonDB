#ifndef PROJECT_COUNTQUERY_H
#define PROJECT_COUNTQUERY_H

#include "../Query.h"

class CountQuery : public ComplexQuery {
  // Define the query name as a constant
  static constexpr const char *qname = "COUNT";

public:
  // Inherit constructors from the ComplexQuery base class
  using ComplexQuery::ComplexQuery;

  // Override the execute method to implement the COUNT logic
  QueryResult::Ptr execute() override;

  // Override the toString method for debugging and logging
  std::string toString() override;
};

#endif // PROJECT_COUNTQUERY_H
