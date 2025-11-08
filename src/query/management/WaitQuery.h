#ifndef WAIT_QUERY_H
#define WAIT_QUERY_H

#include <memory>
#include <string>
#include <utility>

#include "query/Query.h"
#include "query/QueryResult.h"
#include "threading/QueryManager.h"

class WaitQuery : public Query
{
  static constexpr const char* qname = "WAIT";
  std::shared_ptr<std::counting_semaphore<>> target_sem;

public:
  /**
   * Constructor for WAIT query
   * @param sourceTable Name of the table to wait for
   * @param sem Semaphore to wait on for synchronization
   */
  explicit WaitQuery(std::string sourceTable, std::shared_ptr<std::counting_semaphore<>> sem)
      : Query(std::move(sourceTable)), target_sem(std::move(sem))
  {
  }

  /**
   * Execute the WAIT query to synchronize on table operations
   * @return QueryResult with wait operation results
   */
  QueryResult::Ptr execute() override;

  /**
   * Convert query to string representation
   * @return String representation of the WAIT query
   */
  std::string toString() override;

  /**
   * Check if this query modifies data
   * @return Always returns true for WAIT queries (destructive)
   */
  [[nodiscard]] bool isWriter() const override
  {
    return true;
  }

  /**
   * Check if this query must execute immediately
   * @return Always returns true for WAIT queries
   */
  [[nodiscard]] bool isInstant() const override
  {
    return true;
  }
};

#endif // WAIT_QUERY_H
