#ifndef COPY_TABLE_QUERY_H
#define COPY_TABLE_QUERY_H

#include <string>
#include <utility>
#include <vector>

#include "db/Table.h"
#include "query/QueryResult.h"

class CopyTableQuery : public Query
{
  static constexpr const char* qname = "COPYTABLE";
  std::string newTableName;
  constexpr static bool is_multithreaded = false;

private:
  // Helper methods to reduce complexity
  using RowData = std::pair<std::string, std::vector<Table::ValueType>>;

  [[nodiscard]] QueryResult::Ptr validateSourceTable(const Table& src) const;

  [[nodiscard]] std::vector<RowData> static collectSingleThreaded(const Table& src,
                                                           const std::vector<std::string>& fields);

  [[nodiscard]] std::vector<RowData> static collectMultiThreaded(const Table& src,
                                                          const std::vector<std::string>& fields);

public:
  explicit CopyTableQuery(std::string sourceTable, std::string newTable)
      : Query(std::move(sourceTable)), newTableName(std::move(newTable))
  {
  }

  QueryResult::Ptr execute() override;
  std::string toString() override;
};

#endif // COPY_TABLE_QUERY_H
