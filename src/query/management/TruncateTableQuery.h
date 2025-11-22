#ifndef PROJECT_TRUNCATETABLEQUERY_H
#define PROJECT_TRUNCATETABLEQUERY_H

#include <string>

#include "query/Query.h"
#include "query/QueryResult.h"

class TruncateTableQuery : public Query {
public:
  using Query::Query;

  /**
   * Execute the TRUNCATE query to remove all records from table
   * @return QueryResult with truncate operation results
   */
  QueryResult::Ptr execute() override;

  /**
   * Convert query to string representation
   * @return String representation of the TRUNCATE query
   */
  std::string toString() override;
};

#endif  // PROJECT_TRUNCATETABLEQUERY_H
