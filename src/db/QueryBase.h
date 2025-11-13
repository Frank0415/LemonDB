#ifndef PROJECT_QUERY_BASE_H
#define PROJECT_QUERY_BASE_H

#include <memory>
#include <string>

#include "query/QueryResult.h"

class Query {
  // private:
  //   int id = -1;

private:
  std::string targetTable;

public:
  Query() = default;

  [[nodiscard]] std::string &targetTableRef() { return targetTable; }

  // Const overload so const member functions can access target table name
  [[nodiscard]] const std::string &targetTableRef() const {
    return targetTable;
  }

  /**
   * Constructor for Query with target table
   * @param targetTable The name of the target table for this query
   */
  explicit Query(std::string targetTable)
      : targetTable(std::move(targetTable)) {}

  using Ptr = std::unique_ptr<Query>;

  virtual QueryResult::Ptr execute() = 0;

  virtual std::string toString() = 0;

  virtual ~Query() = default;

  // For thread safety: indicate if this query modifies data
  [[nodiscard]] virtual bool isWriter() const { return false; }

  // For execution order: indicate if this query must execute immediately (not
  // parallel) e.g., LOAD and QUIT must execute serially
  [[nodiscard]] virtual bool isInstant() const { return false; }
};

#endif