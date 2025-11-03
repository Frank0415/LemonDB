#ifndef PROJECT_SUBQUERY_H
#define PROJECT_SUBQUERY_H

#include <string>

#include "../Query.h"
#include "../QueryResult.h"

class SubQuery : public ComplexQuery
{
  static constexpr const char* qname = "SUB";

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

  [[nodiscard]] bool isWriter() const override
  {
    return true;
  }
};

#endif // PROJECT_SUBQUERY_H
