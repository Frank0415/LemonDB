#ifndef PROJECT_DELETEQUERY_H
#define PROJECT_DELETEQUERY_H

#include <string>

#include "../../db/Table.h"
#include "../Query.h"
#include "../QueryResult.h"

class DeleteQuery : public ComplexQuery
{
  static constexpr const char* qname = "DELETE";

  [[nodiscard]] QueryResult::Ptr executeSingleThreaded(Table& table);

  [[nodiscard]] QueryResult::Ptr executeMultiThreaded(Table& table);

public:
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute() override;

  std::string toString() override;

  [[nodiscard]] bool isWriter() const override
  {
    return true;
  }
};

#endif // PROJECT_DELETEQUERY_H
