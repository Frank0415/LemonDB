#include "AddQuery.h"

#include <cstddef>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>

#include "../../db/Database.h"
#include "../../utils/formatter.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"

QueryResult::Ptr AddQuery::execute()
{
  using std::string_literals::operator""s;
  try
  {
    if (this->operands.size() < 2)
    {
      return std::make_unique<ErrorMsgResult>(
          qname, this->targetTable, "Invalid number of operands (? operands)."_f % operands.size());
    }
    auto& database = Database::getInstance();
    auto& table = database[this->targetTable];

    auto result = initCondition(table);
    if (!result.second)
    {
      // No valid conditions, return 0
      return std::make_unique<RecordCountResult>(0);
    }
    // this operands stores a list of ADD ( fields ... destField ) FROM table
    // WHERE ( cond ) ...;

    // Stategy: iterate through all rows that satisfy the condition and sum all
    // @ once
    int count = 0;

    for (auto it = table.begin(); it != table.end(); ++it)
    {
      if (!this->evalCondition(*it))
      {
        continue;
      }
      // perform ADD operation
      int sum = 0;
      for (size_t i = 0; i < this->operands.size() - 1; ++i)
      {
        auto fieldIndex = table.getFieldIndex(this->operands[i]);
        sum += (*it)[fieldIndex];
      }
      (*it)[table.getFieldIndex(this->operands.back())] = sum;
      count++;
    }
    return std::make_unique<RecordCountResult>(count);
  }
  catch (const NotFoundKey& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable, "Key not found."s);
  }
  catch (const TableNameNotFound& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable, "No such table."s);
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
                                            "Unkonwn error '?'."_f % e.what());
  }
}

std::string AddQuery::toString()
{
  return "QUERY = ADD TABLE \"" + this->targetTable + "\"";
}
