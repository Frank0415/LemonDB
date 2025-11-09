//
// Created by liu on 18-10-25.
//

#ifndef PROJECT_DROPTABLEQUERY_H
#define PROJECT_DROPTABLEQUERY_H

#include <string>

#include "query/Query.h"
#include "query/QueryResult.h"

class DropTableQuery : public Query
{
  static constexpr const char* qname = "DROP";

public:
  using Query::Query;

  QueryResult::Ptr execute() override;

  std::string toString() override;

  [[nodiscard]] bool isWriter() const override { return true; }
  [[nodiscard]] bool isInstant() const override { return true; }
};

#endif // PROJECT_DROPTABLEQUERY_H
