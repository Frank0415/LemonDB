#ifndef PROJECT_SELECT_QUERY_H
#define PROJECT_SELECT_QUERY_H

#include <string>
#include <vector>

#include "../Query.h"
#include "../QueryResult.h"

class SelectQuery : public ComplexQuery {
  static constexpr const char *qname = "SELECT";

private:
  /**
   * Validate operands for SELECT query
   * @return QueryResult indicating validation success or failure
   */
  [[nodiscard]] QueryResult::Ptr validateOperands() const;

  /**
   * Get field indices for the columns to select
   * @param table The table to get field indices from
   * @return Vector of field indices
   */
  [[nodiscard]] std::vector<Table::FieldIndex>
  getFieldIndices(Table &table) const;

  /**
   * Execute SELECT operation using single-threaded approach
   * @param table The table to select from
   * @param fieldIds Field indices for the operation
   * @return QueryResult with selected records
   */
  [[nodiscard]] QueryResult::Ptr
  executeSingleThreaded(Table &table,
                        const std::vector<Table::FieldIndex> &fieldIds);

  /**
   * Execute SELECT operation using multi-threaded approach
   * @param table The table to select from
   * @param fieldIds Field indices for the operation
   * @return QueryResult with selected records
   */
  [[nodiscard]] QueryResult::Ptr
  executeMultiThreaded(Table &table,
                       const std::vector<Table::FieldIndex> &fieldIds);

public:
  using ComplexQuery::ComplexQuery;
  QueryResult::Ptr execute() override;
  std::string toString() override;
};

#endif  // PROJECT_SELECT_QUERY_H
