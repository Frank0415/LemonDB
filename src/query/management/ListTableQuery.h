//
// Created by liu on 18-10-25.
//

#ifndef PROJECT_LISTTABLEQUERY_H
#define PROJECT_LISTTABLEQUERY_H

#include <string>

#include "query/Query.h"

class ListTableQuery : public Query
{
  static constexpr const char* qname = "LIST";

public:
  /**
   * Execute the LIST query to show available tables
   * @return QueryResult with list of tables
   */
  QueryResult::Ptr execute() override;

  /**
   * Convert query to string representation
   * @return String representation of the LIST query
   */
  std::string toString() override;
};

#endif // PROJECT_LISTTABLEQUERY_H
