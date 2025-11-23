//
// Created by liu on 18-10-25.
//

#ifndef PROJECT_DROPTABLEQUERY_H
#define PROJECT_DROPTABLEQUERY_H

#include <string>

#include "../../db/QueryBase.h"
#include "../QueryResult.h"

class DropTableQuery : public Query {
  static constexpr const char *qname = "DROP";

public:
  using Query::Query;

  /**
   * Execute the DROP query to remove table from database
   * @return QueryResult with drop operation results
   */
  QueryResult::Ptr execute() override;

  /**
   * Convert query to string representation
   * @return String representation of the DROP query
   */
  std::string toString() override;

  /**
   * Check if this query modifies data
   * @return Always returns true for DROP queries
   */
  [[nodiscard]] bool isWriter() const override { return true; }

  /**
   * Check if this query must execute immediately
   * @return Always returns true for DROP queries
   */
  [[nodiscard]] bool isInstant() const override { return true; }
};

#endif  // PROJECT_DROPTABLEQUERY_H
