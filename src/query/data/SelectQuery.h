#ifndef PROJECT_SELECT_QUERY_H
#define PROJECT_SELECT_QUERY_H
#include <map>
#include <string>
#include <vector>

#include "../Query.h"
#include "../QueryResult.h"

class SelectQuery : public ComplexQuery
{
  static constexpr const char* qname = "SELECT";

private:
  [[nodiscard]] QueryResult::Ptr validateOperands() const;

  [[nodiscard]] std::vector<Table::FieldIndex> getFieldIndices(Table& table) const;

  [[nodiscard]] QueryResult::Ptr
  executeSingleThreaded(Table& table, const std::vector<Table::FieldIndex>& fieldIds);

  [[nodiscard]] QueryResult::Ptr
  executeMultiThreaded(Table& table, const std::vector<Table::FieldIndex>& fieldIds);

public:
  using ComplexQuery::ComplexQuery;
  QueryResult::Ptr execute() override;
  std::string toString() override;
};

#endif // PROJECT_SELECT_QUERY_H
