#ifndef PROJECT_MAXQUERY_H
#define PROJECT_MAXQUERY_H

#include <string>

#include "../Query.h"

class MaxQuery : public ComplexQuery {
  static constexpr const char *qname = "MAX";

public:
  using ComplexQuery::ComplexQuery;

  /**
   * Validate operands for MAX query
   * @return QueryResult indicating validation success or failure
   */
  [[nodiscard]] QueryResult::Ptr validateOperands() const;

  /**
   * Get field indices for the columns involved in MAX operation
   * @param table The table to get field indices from
   * @return Vector of field indices
   */
  [[nodiscard]] std::vector<Table::FieldIndex>
  getFieldIndices(const Table &table) const;

  /**
   * Execute MAX operation using single-threaded approach
   * @param table The table to find max values in
   * @param fids Field indices for the operation
   * @return QueryResult with max results
   */
  [[nodiscard]] QueryResult::Ptr
  executeSingleThreaded(const Table &table,
                        const std::vector<Table::FieldIndex> &fids);

  /**
   * Execute MAX operation using multi-threaded approach
   * @param table The table to find max values in
   * @param fids Field indices for the operation
   * @return QueryResult with max results
   */
  [[nodiscard]] QueryResult::Ptr
  executeMultiThreaded(const Table &table,
                       const std::vector<Table::FieldIndex> &fids);

  QueryResult::Ptr execute() override;

  std::string toString() override;
};

#endif  // PROJECT_MAXQUERY_H
