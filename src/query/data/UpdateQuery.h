//
// Created by liu on 18-10-25.
//

#ifndef PROJECT_UPDATEQUERY_H
#define PROJECT_UPDATEQUERY_H

#include <string>
#include <utility>
#include <vector>

#include "../Query.h"

class UpdateQuery : public ComplexQuery
{
  static constexpr const char* qname = "UPDATE";
  Table::ValueType fieldValue = 0;
  Table::FieldIndex fieldId = 0;
  Table::KeyType keyValue;

public:
  UpdateQuery(std::string table,
              std::vector<std::string> operands,
              std::vector<QueryCondition> conditions)
      : ComplexQuery(std::move(table), std::move(operands), std::move(conditions))
  {
  }

  QueryResult::Ptr execute() override;

  std::string toString() override;

  [[nodiscard]] bool isWriter() const override { return true; }
};

#endif // PROJECT_UPDATEQUERY_H
