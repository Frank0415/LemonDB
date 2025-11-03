#ifndef PROJECT_SWAPQUERY_H
#define PROJECT_SWAPQUERY_H

#include <string>

#include "../Query.h"
#include "db/Table.h"
class SwapQuery : public ComplexQuery
{
  static constexpr const char* qname = "SWAP";

public:
  [[nodiscard]] QueryResult::Ptr validateOperands() const;

  [[nodiscard]] std::pair<const Table::FieldIndex, const Table::FieldIndex>
  getFieldIndices(Table& table) const;
  
  using ComplexQuery::ComplexQuery;
  QueryResult::Ptr execute() override;
  std::string toString() override;
  [[nodiscard]] bool isWriter() const override
  {
    return true;
  }
};

#endif // PROJECT_SWAPQUERY_H
