//
// Created by liu on 18-10-25.
//

#ifndef PROJECT_INSERTQUERY_H
#define PROJECT_INSERTQUERY_H

#include <string>

#include "../Query.h"

class InsertQuery : public ComplexQuery
{
  static constexpr const char* qname = "INSERT";

public:
  using ComplexQuery::ComplexQuery;

  /**
   * Execute the INSERT query to add records to table
   * @return QueryResult with execution results
   */
  QueryResult::Ptr execute() override;

  /**
   * Convert query to string representation
   * @return String representation of the INSERT query
   */
  std::string toString() override;

  /**
   * Check if this query modifies data
   * @return Always returns true for INSERT queries
   */
  [[nodiscard]] bool isWriter() const override
  {
    return true;
  }
};

#endif // PROJECT_INSERTQUERY_H
