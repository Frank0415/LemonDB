//
// Created by liu on 18-10-25.
//

#ifndef PROJECT_LOADTABLEQUERY_H
#define PROJECT_LOADTABLEQUERY_H

#include <string>
#include <utility>

#include "query/Query.h"
#include "query/QueryResult.h"

class LoadTableQuery : public Query
{
  static constexpr const char* qname = "LOAD";
  std::string fileName;

public:
  /**
   * Constructor for LOAD table query
   * @param table Name of the table to load
   * @param fileName File path to load data from
   */
  explicit LoadTableQuery(std::string table, std::string fileName)
      : Query(std::move(table)), fileName(std::move(fileName))
  {
  }

  /**
   * Execute the LOAD query to load table data from file
   * @return QueryResult with load operation results
   */
  QueryResult::Ptr execute() override;

  /**
   * Convert query to string representation
   * @return String representation of the LOAD query
   */
  std::string toString() override;

  /**
   * Check if this query modifies data
   * @return Always returns true for LOAD queries
   */
  [[nodiscard]] bool isWriter() const override
  {
    return true;
  }

  /**
   * Check if this query must execute immediately and serially
   * @return Always returns true for LOAD queries
   */
  [[nodiscard]] bool isInstant() const override
  {
    return true;
  }
};

#endif // PROJECT_LOADTABLEQUERY_H
