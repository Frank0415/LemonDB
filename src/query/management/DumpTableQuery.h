//
// Created by liu on 18-10-25.
//

#ifndef PROJECT_DUMPTABLEQUERY_H
#define PROJECT_DUMPTABLEQUERY_H

#include <string>
#include <utility>

#include "query/Query.h"

class DumpTableQuery : public Query {
  static constexpr const char *qname = "DUMP";
  std::string fileName;

public:
  /**
   * Constructor for DUMP table query
   * @param table Name of the table to dump
   * @param filename File path to save data to
   */
  DumpTableQuery(std::string table, std::string filename)
      : Query(std::move(table)), fileName(std::move(filename)) {}

  /**
   * Execute the DUMP query to save table data to file
   * @return QueryResult with dump operation results
   */
  QueryResult::Ptr execute() override;

  /**
   * Convert query to string representation
   * @return String representation of the DUMP query
   */
  std::string toString() override;

  /**
   * Check if this query modifies data
   * @return Always returns false for DUMP queries (read-only)
   */
  [[nodiscard]] bool isWriter() const override {
    return false;
  }  // DUMP only reads
  /**
   * Check if this query must execute immediately and serially
   * @return Always returns true for DUMP queries
   */
  [[nodiscard]] bool isInstant() const override {
    return true;
  }  // But must be serial
};

#endif  // PROJECT_DUMPTABLEQUERY_H
