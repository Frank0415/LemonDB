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
  Table::ValueType fieldValue; // = (operands[0]=="KEY")? 0 :std::stoi(operands[1]);
  Table::FieldIndex fieldId;
  Table::KeyType keyValue;

public:
  UpdateQuery(std::string table,
              std::vector<std::string> operands,
              std::vector<QueryCondition> conditions)
      : ComplexQuery(std::move(table), std::move(operands), std::move(conditions)), fieldValue(0),
        fieldId(0)
  {
  }

  QueryResult::Ptr execute() override;

  std::string toString() override;
};

#endif // PROJECT_UPDATEQUERY_H
