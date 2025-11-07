#include "WaitQuery.h"

#include "query/QueryResult.h"

// Custom exception for WaitQuery completion
class WaitQueryCompleted : public std::exception
{
public:
  const char* what() const noexcept override
  {
    return "WaitQuery completed - queries can now proceed on new table";
  }
};

QueryResult::Ptr WaitQuery::execute()
{
  // Block until the semaphore is released (COPY/LOAD completed)
  target_sem->acquire();

  // Throw exception to indicate this query should be ignored by manager
  throw WaitQueryCompleted();
}

std::string WaitQuery::toString()
{
  return "QUERY = WAIT, TABLE = \"" + this->targetTableRef() + "\"";
}
