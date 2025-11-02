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
  [[nodiscard]] QueryResult::Ptr validateOperands() const;

  [[nodiscard]] std::vector<Table::FieldIndex> getFieldIndices(const Table& table) const;

  [[nodiscard]] QueryResult::Ptr executeSingleThreaded(Table& table,
                                                       const std::vector<Table::FieldIndex>& fids);

  [[nodiscard]] QueryResult::Ptr executeMultiThreaded(Table& table,
                                                      const std::vector<Table::FieldIndex>& fids);

public:
  using ComplexQuery::ComplexQuery;
  QueryResult::Ptr execute() override;
  std::string toString() override;
};
#endif // PROJECT_SUMQUERY_H
