#ifndef PROJECT_SUMQUERY_H
#define PROJECT_SUMQUERY_H

#include <string>
#include <vector>

#include "../../db/Table.h"
#include "../Query.h"
#include "../QueryResult.h"

class SumQuery : public ComplexQuery
{
  static constexpr const char* qname = "SUM";

private:
  // Helper methods to reduce complexity
  /**
   * Validate operands for SUM query
   * @return QueryResult indicating validation success or failure
   */
  [[nodiscard]] QueryResult::Ptr validateOperands() const;

  /**
   * Get field indices for the columns to sum
   * @param table The table to get field indices from
   * @return Vector of field indices
   */
  [[nodiscard]] std::vector<Table::FieldIndex> getFieldIndices(const Table& table) const;

  /**
   * Execute SUM operation using single-threaded approach
   * @param table The table to sum values in
   * @param fids Field indices for the operation
   * @return QueryResult with sum results
   */
  [[nodiscard]] QueryResult::Ptr executeSingleThreaded(Table& table,
                                                       const std::vector<Table::FieldIndex>& fids);

  /**
   * Execute SUM operation using multi-threaded approach
   * @param table The table to sum values in
   * @param fids Field indices for the operation
   * @return QueryResult with sum results
   */
  [[nodiscard]] QueryResult::Ptr executeMultiThreaded(Table& table,
                                                      const std::vector<Table::FieldIndex>& fids);

public:
  using ComplexQuery::ComplexQuery;
  QueryResult::Ptr execute() override;
  std::string toString() override;
};
#endif // PROJECT_SUMQUERY_H
