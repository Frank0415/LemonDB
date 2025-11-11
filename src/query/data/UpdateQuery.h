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

private:
  /**
   * Validate operands for UPDATE query
   * @return QueryResult indicating validation success or failure
   */
  [[nodiscard]] QueryResult::Ptr validateOperands() const;

  /**
   * Execute UPDATE operation using single-threaded approach
   * @param table The table to update records in
   * @return QueryResult with update results
   */
  [[nodiscard]] QueryResult::Ptr executeSingleThreaded(Table& table);

  /**
   * Execute UPDATE operation using multi-threaded approach
   * @param table The table to update records in
   * @return QueryResult with update results
   */
  [[nodiscard]] QueryResult::Ptr executeMultiThreaded(Table& table);

public:
  /**
   * Construct an UpdateQuery with table, operands, and conditions
   * @param table The name of the target table
   * @param operands The operands for the update
   * @param conditions The conditions for the update
   */
  UpdateQuery(std::string table,
              std::vector<std::string> operands,
              std::vector<QueryCondition> conditions)
      : ComplexQuery(std::move(table), std::move(operands), std::move(conditions))
  {
  }

  QueryResult::Ptr execute() override;

  std::string toString() override;

  [[nodiscard]] bool isWriter() const override
  {
    return true;
  }
};

#endif // PROJECT_UPDATEQUERY_H
