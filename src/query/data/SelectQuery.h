#ifndef PROJECT_SELECT_QUERY_H
#define PROJECT_SELECT_QUERY_H
#include "../Query.h"

class SelectQuery : public ComplexQuery {
  static constexpr const char* qname = "SELECT";
public:
  using ComplexQuery::ComplexQuery;
  QueryResult::Ptr execute() override;
  std::string toString() override {
    return "QUERY = SELECT \"" + this->targetTable + "\"";
  }
};

#endif // PROJECT_SELECT_QUERY_H