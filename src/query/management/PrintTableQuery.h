//
// Created by liu on 18-10-25.
//

#ifndef PROJECT_PRINTTABLEQUERY_H
#define PROJECT_PRINTTABLEQUERY_H

#include <string>

#include "query/Query.h"
#include "query/QueryResult.h"

class PrintTableQuery : public Query {
  static constexpr const char *qname = "SHOWTABLE";

public:
  using Query::Query;

  /**
   * Execute the SHOWTABLE query to display table contents
   * @return QueryResult with table data for display
   */
  QueryResult::Ptr execute() override;

  /**
   * Convert query to string representation
   * @return String representation of the SHOWTABLE query
   */
  std::string toString() override;
};

#endif  // PROJECT_PRINTTABLEQUERY_H
