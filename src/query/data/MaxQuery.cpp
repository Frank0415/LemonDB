#include "MaxQuery.h"

#include <algorithm>
#include <cstddef>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "../../db/Database.h"
#include "../../db/Table.h"
#include "../../db/TableLockManager.h"
#include "../../utils/formatter.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"

QueryResult::Ptr MaxQuery::execute()
{
  using std::string_literals::operator""s;

  try
  {
    if (validateOperands() != nullptr)
    {
      return validateOperands();
    }

    Database& database = Database::getInstance();
    auto lock = TableLockManager::getInstance().acquireRead(this->targetTable);
    auto& table = database[this->targetTable];

    auto fieldId = getFieldIndices(table);

    auto result = initCondition(table);
    if (result.second)
    {
      bool found = false;
      std::vector<Table::ValueType> maxValue(fieldId.size(),
                                             Table::ValueTypeMin); // each has its own max value

      for (const auto& row : table)
      {
        if (this->evalCondition(row))
        {
          found = true;

          for (size_t i = 0; i < fieldId.size(); ++i)
          {
            maxValue[i] = std::max(maxValue[i], row[fieldId[i]]);
          }
        }
      }

      if (!found)
      {
        return std::make_unique<NullQueryResult>();
      }
      return std::make_unique<SuccessMsgResult>(maxValue);
    }
    return std::make_unique<NullQueryResult>();
  }
  catch (const TableNameNotFound& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable, "No such table."s);
  }
  catch (const TableFieldNotFound& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
  }
  catch (const IllFormedQueryCondition& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
  }
  catch (const std::invalid_argument& e)
  {
    // Cannot convert operand to string
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable,
                                            "Unknown error '?'"_f % e.what());
  }
  catch (const std::exception& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable,
                                            "Unknown error '?'."_f % e.what());
  }
}

std::string MaxQuery::toString()
{
  return "QUERY = MAX " + this->targetTable + "\"";
}

[[nodiscard]] QueryResult::Ptr MaxQuery::validateOperands() const
{
  if (this->operands.empty())
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable.c_str(),
                                            "No operand (? operands)."_f % operands.size());
  }
  return nullptr;
}