#ifndef PROJECT_TRUNCATETABLEQUERY_H
#define PROJECT_TRUNCATETABLEQUERY_H

#include "../Query.h"

class TruncateTableQuery : public Query {
public:
  using Query::Query;
  QueryResult::Ptr execute() override;
  std::string toString() override;
};

#endif // PROJECT_TRUNCATETABLEQUERY_H