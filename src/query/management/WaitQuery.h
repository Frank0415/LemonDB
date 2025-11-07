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
  explicit WaitQuery(std::string sourceTable, std::shared_ptr<std::counting_semaphore<>> sem)
      : Query(std::move(sourceTable)), target_sem(std::move(sem))
  {
  }

  QueryResult::Ptr execute() override;

  std::string toString() override;

  // WaitQuery is destructive - must execute serially
  [[nodiscard]] bool isWriter() const override
  {
    return true;
  }

  // WaitQuery is instant - must execute immediately
  [[nodiscard]] bool isInstant() const override
  {
    return true;
  }
};

#endif // WAIT_QUERY_H
