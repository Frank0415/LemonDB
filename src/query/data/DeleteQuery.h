#ifndef PROJECT_DELETEQUERY_H
#define PROJECT_DELETEQUERY_H

#include <string>

#include "../Query.h"

class DeleteQuery : public ComplexQuery
{
  static constexpr const char* qname = "DELETE";

public:
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute() override;

  std::string toString() override;

  [[nodiscard]] bool isWriter() const override { return true; }
};

#endif // PROJECT_DELETEQUERY_H
