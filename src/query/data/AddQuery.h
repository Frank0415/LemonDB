#ifndef PROJECT_ADDQUERY_H
#define PROJECT_ADDQUERY_H

#include <string>
#include <vector>

#include "../../db/Table.h"
#include "../Query.h"
#include "../QueryResult.h"

class AddQuery : public ComplexQuery {
  static constexpr const char *qname = "ADD";

public:
  /**
   * Validate that operands are correct for ADD operation
   * @return QueryResult indicating success or failure with error message
   */
  [[nodiscard]] QueryResult::Ptr validateOperands() const;

  /**
   * Get field indices for the columns involved in ADD operation
   * @param table The table to get field indices from
   * @return Vector of field indices
   */
  [[nodiscard]] std::vector<Table::FieldIndex>
  getFieldIndices(const Table &table) const;

  /**
   * Execute ADD operation using single-threaded approach
   * @param table The table to modify
   * @param fids Field indices for the operation
   * @return QueryResult with operation results
   */
  [[nodiscard]] QueryResult::Ptr
  executeSingleThreaded(Table &table,
                        const std::vector<Table::FieldIndex> &fids);

  /**
   * Execute ADD operation using multi-threaded approach
   * @param table The table to modify
   * @param fids Field indices for the operation
   * @return QueryResult with operation results
   */
  [[nodiscard]] QueryResult::Ptr
  executeMultiThreaded(Table &table,
                       const std::vector<Table::FieldIndex> &fids);

  using ComplexQuery::ComplexQuery;

  /**
   * Execute the ADD query
   * @return QueryResult with execution results
   */
  [[nodiscard]] QueryResult::Ptr execute() override;

  /**
   * Convert query to string representation
   * @return String representation of the query
   */
  [[nodiscard]] std::string toString() override;

  /**
   * Check if this query modifies data
   * @return Always returns true for ADD queries
   */
  [[nodiscard]] bool isWriter() const override { return true; }
};

#endif
