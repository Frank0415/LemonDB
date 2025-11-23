#ifndef PROJECT_DUPLICATEQUERY_H
#define PROJECT_DUPLICATEQUERY_H
#include <string>
#include <utility>
#include <vector>

#include "../Query.h"
#include "../../db/Table.h"

class DuplicateQuery : public ComplexQuery {
  static constexpr const char *qname = "DUPLICATE";

  // Type alias for records to duplicate
  using RecordPair = std::pair<Table::KeyType, std::vector<Table::ValueType>>;

private:
  /**
   * Validate operands for DUPLICATE query
   * @return QueryResult indicating validation success or failure
   */
  [[nodiscard]] QueryResult::Ptr validateOperands() const;

  /**
   * Execute DUPLICATE operation using single-threaded approach
   * @param table The table to duplicate records from
   * @return Vector of record pairs to duplicate
   */
  [[nodiscard]] std::vector<RecordPair> executeSingleThreaded(Table &table);

  /**
   * Execute DUPLICATE operation using multi-threaded approach
   * @param table The table to duplicate records from
   * @return Vector of record pairs to duplicate
   */
  [[nodiscard]] std::vector<RecordPair> executeMultiThreaded(Table &table);

public:
  using ComplexQuery::ComplexQuery;
  QueryResult::Ptr execute() override;
  std::string toString() override;
  [[nodiscard]] bool isWriter() const override { return true; }
};

#endif  // PROJECT_DUPLICATEQUERY_H
