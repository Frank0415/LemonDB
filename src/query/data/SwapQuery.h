#ifndef PROJECT_SWAPQUERY_H
#define PROJECT_SWAPQUERY_H

#include <string>

#include "../Query.h"
#include "db/Table.h"
class SwapQuery : public ComplexQuery {
  static constexpr const char *qname = "SWAP";

public:
  /**
   * Validate operands for SWAP query
   * @return QueryResult indicating validation success or failure
   */
  [[nodiscard]] QueryResult::Ptr validateOperands() const;

  /**
   * Get field indices for the two columns to swap
   * @param table The table to get field indices from
   * @return Pair of field indices for the two columns
   */
  [[nodiscard]] std::pair<const Table::FieldIndex, const Table::FieldIndex>
  getFieldIndices(const Table &table) const;

  /**
   * Execute SWAP operation using single-threaded approach
   * @param table The table to modify
   * @param field_index_1 Index of first field to swap
   * @param field_index_2 Index of second field to swap
   * @return QueryResult with operation results
   */
  [[nodiscard]] QueryResult::Ptr
  executeSingleThreaded(Table &table, const Table::FieldIndex &field_index_1,
                        const Table::FieldIndex &field_index_2);

  /**
   * Execute SWAP operation using multi-threaded approach
   * @param table The table to modify
   * @param field_index_1 Index of first field to swap
   * @param field_index_2 Index of second field to swap
   * @return QueryResult with operation results
   */
  [[nodiscard]] QueryResult::Ptr
  executeMultiThreaded(Table &table, const Table::FieldIndex &field_index_1,
                       const Table::FieldIndex &field_index_2);

  using ComplexQuery::ComplexQuery;
  QueryResult::Ptr execute() override;
  std::string toString() override;
  [[nodiscard]] bool isWriter() const override { return true; }
};

#endif  // PROJECT_SWAPQUERY_H
