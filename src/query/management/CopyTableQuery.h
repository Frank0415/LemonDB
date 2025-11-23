#ifndef COPY_TABLE_QUERY_H
#define COPY_TABLE_QUERY_H

#include <memory>
#include <semaphore>
#include <string>
#include <utility>
#include <vector>

#include "../../db/Table.h"
#include "../../db/QueryBase.h"
#include "../QueryResult.h"

class CopyTableQuery : public Query {
  static constexpr const char *qname = "COPYTABLE";
  std::string newTableName;
  constexpr static bool is_multithreaded = false;
  std::shared_ptr<std::counting_semaphore<>> wait_sem;

private:
  // Helper methods to reduce complexity
  using RowData = std::pair<std::string, std::vector<Table::ValueType>>;

  /**
   * Validate that the source table exists and is accessible
   * @param src The source table to validate
   * @return QueryResult indicating validation success or failure
   */
  [[nodiscard]] QueryResult::Ptr validateSourceTable(const Table &src) const;

  /**
   * Collect row data from source table using single-threaded approach
   * @param src The source table to collect from
   * @param fields The fields to collect
   * @return Vector of row data pairs
   */
  [[nodiscard]] std::vector<RowData> static collectSingleThreaded(
      const Table &src, const std::vector<std::string> &fields);

  /**
   * Collect row data from source table using multi-threaded approach
   * @param src The source table to collect from
   * @param fields The fields to collect
   * @return Vector of row data pairs
   */
  [[nodiscard]] std::vector<RowData> static collectMultiThreaded(
      const Table &src, const std::vector<std::string> &fields);

public:
  /**
   * Constructor for COPYTABLE query
   * @param sourceTable Name of the source table to copy from
   * @param newTable Name of the new table to create
   */
  explicit CopyTableQuery(std::string sourceTable, std::string newTable)
      : Query(std::move(sourceTable)), newTableName(std::move(newTable)),
        wait_sem(std::make_shared<std::counting_semaphore<>>(0)) {}

  /**
   * Execute the COPYTABLE query to duplicate a table
   * @return QueryResult with copy operation results
   */
  QueryResult::Ptr execute() override;

  /**
   * Convert query to string representation
   * @return String representation of the COPYTABLE query
   */
  std::string toString() override;

  /**
   * Get the wait semaphore for synchronization with dependent queries
   * @return Shared pointer to the counting semaphore
   */
  [[nodiscard]] std::shared_ptr<std::counting_semaphore<>>
  getWaitSemaphore() const {
    return wait_sem;
  }
};

#endif  // COPY_TABLE_QUERY_H
