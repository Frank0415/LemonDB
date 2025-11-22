#ifndef PROJECT_COUNTQUERY_H
#define PROJECT_COUNTQUERY_H

#include <string>

#include "../../db/Table.h"
#include "../Query.h"
#include "../QueryResult.h"

class CountQuery : public ComplexQuery {
  // Define the query name as a constant
  static constexpr const char *qname = "COUNT";

private:
  /**
   * Validate operands for COUNT query
   * @return QueryResult indicating validation success or failure
   */
  [[nodiscard]] QueryResult::Ptr validateOperands() const;

  /**
   * Execute COUNT operation using single-threaded approach
   * @param table The table to count records in
   * @return QueryResult with count results
   */
  [[nodiscard]] QueryResult::Ptr executeSingleThreaded(Table &table);

  /**
   * Execute COUNT operation using multi-threaded approach
   * @param table The table to count records in
   * @return QueryResult with count results
   */
  [[nodiscard]] QueryResult::Ptr executeMultiThreaded(Table &table);

public:
  // Inherit constructors from the ComplexQuery base class
  using ComplexQuery::ComplexQuery;

  /**
   * Execute the COUNT query
   * @return QueryResult with count of matching records
   */
  QueryResult::Ptr execute() override;

  /**
   * Convert query to string representation
   * @return String representation of the COUNT query
   */
  std::string toString() override;
};

#endif  // PROJECT_COUNTQUERY_H
