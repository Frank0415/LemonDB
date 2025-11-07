#ifndef PROJECT_DUPLICATEQUERY_H
#define PROJECT_DUPLICATEQUERY_H
#include <string>
#include <utility>
#include <vector>

#include "../Query.h"
#include "db/Table.h"

class DuplicateQuery : public ComplexQuery
{
  static constexpr const char* qname = "DUPLICATE";

  // Type alias for records to duplicate
  using RecordPair = std::pair<Table::KeyType, std::vector<Table::ValueType>>;

private:
  [[nodiscard]] QueryResult::Ptr validateOperands() const;

  [[nodiscard]] std::vector<RecordPair> executeSingleThreaded(Table& table);

  [[nodiscard]] std::vector<RecordPair> executeMultiThreaded(Table& table);

public:
  using ComplexQuery::ComplexQuery;
  QueryResult::Ptr execute() override;
  std::string toString() override;
  [[nodiscard]] bool isWriter() const override
  {
    return true;
  }
};

#endif // PROJECT_DUPLICATEQUERY_H
