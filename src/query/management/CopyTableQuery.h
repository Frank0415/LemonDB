#ifndef COPY_TABLE_QUERY_H
#define COPY_TABLE_QUERY_H

#include "../Query.h"

class CopyTableQuery : public Query {
  static constexpr const char *qname = "COPYTABLE";
  const std::string newTableName;

public:
  explicit CopyTableQuery(std::string sourceTable, std::string newTable)
      : Query(std::move(sourceTable)), newTableName(std::move(newTable)) {}

  QueryResult::Ptr execute() override;
  std::string toString() override;
};

#endif // COPY_TABLE_QUERY_H
