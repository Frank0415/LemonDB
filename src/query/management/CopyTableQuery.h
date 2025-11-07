#ifndef COPY_TABLE_QUERY_H
#define COPY_TABLE_QUERY_H

#include <memory>
#include <semaphore>
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
  std::shared_ptr<std::counting_semaphore<>> wait_sem;

private:
  // Helper methods to reduce complexity
  using RowData = std::pair<std::string, std::vector<Table::ValueType>>;

  [[nodiscard]] QueryResult::Ptr validateSourceTable(const Table& src) const;

  [[nodiscard]] std::vector<RowData> static collectSingleThreaded(
      const Table& src, const std::vector<std::string>& fields);

  [[nodiscard]] std::vector<RowData> static collectMultiThreaded(
      const Table& src, const std::vector<std::string>& fields);

public:
  explicit CopyTableQuery(std::string sourceTable, std::string newTable)
      : Query(std::move(sourceTable)), newTableName(std::move(newTable)),
        wait_sem(std::make_shared<std::counting_semaphore<>>(0))
  {
  }

  QueryResult::Ptr execute() override;
  std::string toString() override;

  [[nodiscard]] std::shared_ptr<std::counting_semaphore<>> getWaitSemaphore() const
  {
    return wait_sem;
  }
};

#endif // COPY_TABLE_QUERY_H
