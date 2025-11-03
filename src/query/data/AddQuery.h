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

  [[nodiscard]] QueryResult::Ptr executeSingleThreaded(Table& table);

  using ComplexQuery::ComplexQuery;

  [[nodiscard]] QueryResult::Ptr execute() override;

  [[nodiscard]] std::string toString() override;

  [[nodiscard]] bool isWriter() const override { return true; }
};

#endif
