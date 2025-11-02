#ifndef PROJECT_MAXQUERY_H
#define PROJECT_MAXQUERY_H

#include <string>

#include "../Query.h"

class MaxQuery : public ComplexQuery
{
  static constexpr const char* qname = "MAX";

public:
  using ComplexQuery::ComplexQuery;

  [[nodiscard]] QueryResult::Ptr validateOperands() const;

  [[nodiscard]] std::vector<Table::FieldIndex> getFieldIndices(const Table& table) const;

  [[nodiscard]] QueryResult::Ptr
  executeKeyConditionOptimization(Table& table, const std::vector<Table::FieldIndex>& fids);

  [[nodiscard]] QueryResult::Ptr executeSingleThreaded(Table& table,
                                                       const std::vector<Table::FieldIndex>& fids);

  [[nodiscard]] QueryResult::Ptr executeMultiThreaded(Table& table,
                                                      const std::vector<Table::FieldIndex>& fids);

  QueryResult::Ptr execute() override;

  std::string toString() override;
};

#endif // PROJECT_MAXQUERY_H
