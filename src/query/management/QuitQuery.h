//
// Created by liu on 18-10-25.
//

#ifndef PROJECT_QUITQUERY_H
#define PROJECT_QUITQUERY_H

#include <string>

#include "query/Query.h"
#include "query/QueryResult.h"

class QuitQuery : public Query {
  static constexpr const char *qname = "QUIT";

public:
  /**
   * Default constructor for QUIT query
   */
  QuitQuery() = default;

  /**
   * Execute the QUIT query to terminate the database session
   * @return QueryResult indicating successful termination
   */
  QueryResult::Ptr execute() override;

  /**
   * Convert query to string representation
   * @return String representation of the QUIT query
   */
  std::string toString() override;

  /**
   * Check if this query modifies data
   * @return Always returns false for QUIT queries
   */
  [[nodiscard]] bool isWriter() const override { return false; }

  /**
   * Check if this query must execute immediately and serially
   * @return Always returns true for QUIT queries
   */
  [[nodiscard]] bool isInstant() const override { return true; }
};

#endif  // PROJECT_QUITQUERY_H
