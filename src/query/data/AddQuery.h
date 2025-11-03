#ifndef PROJECT_ADDQUERY_H
#define PROJECT_ADDQUERY_H

#include <string>

#include "../Query.h"
#include "../QueryResult.h"

class AddQuery : public ComplexQuery
{
  static constexpr const char* qname = "ADD";

public:
  [[nodiscard]] QueryResult::Ptr validateOperands() const;

  [[nodiscard]] std::vector<Table::FieldIndex> getFieldIndices(const Table& table) const;

  [[nodiscard]] QueryResult::Ptr executeSingleThreaded(Table& table,
                                                       const std::vector<Table::FieldIndex>& fids);

  [[nodiscard]] QueryResult::Ptr executeMultiThreaded(Table& table,
                                                      const std::vector<Table::FieldIndex>& fids);

  using ComplexQuery::ComplexQuery;

  [[nodiscard]] QueryResult::Ptr execute() override;

  [[nodiscard]] std::string toString() override;

  [[nodiscard]] bool isWriter() const override
  {
    return true;
  }
};

#endif
