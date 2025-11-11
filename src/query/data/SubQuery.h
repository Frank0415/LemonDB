#ifndef PROJECT_SUBQUERY_H
#define PROJECT_SUBQUERY_H

#include <string>

#include "../Query.h"
#include "../QueryResult.h"

class SubQuery : public ComplexQuery
{
  static constexpr const char* qname = "SUB";

private:
  /**
   * Validate operands for SUB query
   * @return QueryResult indicating validation success or failure
   */
  [[nodiscard]] QueryResult::Ptr validateOperands() const;

  /**
   * Get field indices for the columns involved in SUB operation
   * @param table The table to get field indices from
   * @return Vector of field indices
   */
  [[nodiscard]] std::vector<Table::FieldIndex> getFieldIndices(const Table& table) const;

  /**
   * Execute SUB operation using single-threaded approach
   * @param table The table to modify
   * @param fids Field indices for the operation
   * @return QueryResult with operation results
   */
  [[nodiscard]] QueryResult::Ptr executeSingleThreaded(Table& table,
                                                       const std::vector<Table::FieldIndex>& fids);

  /**
   * Execute SUB operation using multi-threaded approach
   * @param table The table to modify
   * @param fids Field indices for the operation
   * @return QueryResult with operation results
   */
  [[nodiscard]] QueryResult::Ptr executeMultiThreaded(Table& table,
                                                      const std::vector<Table::FieldIndex>& fids);

public:
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute() override;

  std::string toString() override;

  [[nodiscard]] bool isWriter() const override
  {
    return true;
  }
};

#endif // PROJECT_SUBQUERY_H
