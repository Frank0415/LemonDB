#include "MaxQuery.h"

#include <cstddef>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "../../db/Database.h"
#include "../../db/Table.h"
#include "../../utils/formatter.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"

QueryResult::Ptr MaxQuery::execute()
{
  using namespace std;
  if (this->operands.empty())
  {
    return make_unique<ErrorMsgResult>(qname, this->targetTable.c_str(),
                                       "No operand (? operands)."_f % operands.size());
  }

  try
  {
    Database& db = Database::getInstance();
    auto& table = db[this->targetTable];

    vector<Table::FieldIndex> fieldId; // transform into its own Id, avoid lookups in map everytime

    try
    {
      for (const auto& operand : this->operands)
      {
        if (operand == "KEY")
        {
          throw IllFormedQueryCondition("MAX operation not supported on KEY field.");
        }
        fieldId.push_back(table.getFieldIndex(operand));
      }
    }
    catch (const TableFieldNotFound& e)
    {
      return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
    }
    catch (const exception& e)
    {
      return make_unique<ErrorMsgResult>(qname, this->targetTable,
                                         "Unkonwn error '?'."_f % e.what());
    }

    try
    {
      auto result = initCondition(table);
      if (result.second)
      {
        bool found = false;
        vector<Table::ValueType> maxValue(fieldId.size(),
                                          Table::ValueTypeMin); // each has its own max value

        for (auto it = table.begin(); it != table.end(); ++it)
        {
          if (this->evalCondition(*it))
          {
            found = true;

            for (size_t i = 0; i < fieldId.size(); ++i)
            {
              if ((*it)[fieldId[i]] > maxValue[i])
              {
                maxValue[i] = (*it)[fieldId[i]];
              }
            }
          }
        }

        if (found == false)
        {
          return make_unique<NullQueryResult>();
        }
        return make_unique<SuccessMsgResult>(maxValue);
      }
      return make_unique<NullQueryResult>();
    }
    catch (const IllFormedQueryCondition& e)
    {
      return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
    }
    catch (const invalid_argument& e)
    {
      // Cannot convert operand to string
      return make_unique<ErrorMsgResult>(qname, this->targetTable,
                                         "Unknown error '?'"_f % e.what());
    }
    catch (const exception& e)
    {
      return make_unique<ErrorMsgResult>(qname, this->targetTable,
                                         "Unkonwn error '?'."_f % e.what());
    }
  }
  catch (const TableNameNotFound& e)
  {
    return make_unique<ErrorMsgResult>(qname, this->targetTable, "No such table."s);
  }
}

std::string MaxQuery::toString()
{
  return "QUERY = MAX " + this->targetTable + "\"";
}
