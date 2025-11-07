#ifndef PROJECT_MINQUERY_H
#define PROJECT_MINQUERY_H

#include <string>

#include "../Query.h"

class MinQuery : public ComplexQuery
{
  static constexpr const char* qname = "MIN";

private:
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

#endif // PROJECT_MINQUERY_H
