#ifndef PROJECT_MINQUERY_H
#define PROJECT_MINQUERY_H

#include <string>
#include <vector>

#include "../../db/Table.h"
#include "../Query.h"
#include "../QueryResult.h"

class MinQuery : public ComplexQuery {
  static constexpr const char *qname = "MIN";

private:
  /**
   * Validate operands for MIN query
   * @return QueryResult indicating validation success or failure
   */
  [[nodiscard]] QueryResult::Ptr validateOperands() const;

  /**
   * Get field indices for the columns involved in MIN operation
   * @param table The table to get field indices from
   * @return Vector of field indices
   */
  [[nodiscard]] std::vector<Table::FieldIndex>
  getFieldIndices(const Table &table) const;

  /**
   * Execute MIN operation using single-threaded approach
   * @param table The table to find min values in
   * @param fids Field indices for the operation
   * @return QueryResult with min results
   */
  [[nodiscard]] QueryResult::Ptr
  executeSingleThreaded(const Table &table,
                        const std::vector<Table::FieldIndex> &fids);

  /**
   * Execute MIN operation using multi-threaded approach
   * @param table The table to find min values in
   * @param fids Field indices for the operation
   * @return QueryResult with min results
   */
  [[nodiscard]] QueryResult::Ptr
  executeMultiThreaded(const Table &table,
                       const std::vector<Table::FieldIndex> &fids);

public:
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute() override;

  std::string toString() override;
};

#endif  // PROJECT_MINQUERY_H
