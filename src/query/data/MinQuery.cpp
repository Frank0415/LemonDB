#include "MinQuery.h"

#include <memory>
#include <string>
#include <vector>

#include "../../db/Database.h"
#include "../QueryResult.h"

constexpr const char* MinQuery::qname;

QueryResult::Ptr MinQuery::execute()
{
  using namespace std;
  if (this->operands.empty())
  {
    return make_unique<ErrorMsgResult>(qname, this->targetTable.c_str(),
                                       "No operand (? operands)."_f % operands.size());
  }
  Database& db = Database::getInstance();
  try
  {
    auto& table = db[this->targetTable];

    vector<Table::FieldIndex> fieldId; // transform into its own Id, avoid lookups in map everytime

    try
    {
      for (const auto& operand : this->operands)
      {
        if (operand == "KEY")
        {
          throw IllFormedQueryCondition("MIN operation not supported on KEY field.");
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
        vector<Table::ValueType> minValue(fieldId.size(),
                                          Table::ValueTypeMax); // each has its own min value

        for (auto it = table.begin(); it != table.end(); ++it)
        {
          if (this->evalCondition(*it))
          {
            found = true;

            for (size_t i = 0; i < fieldId.size(); ++i)
            {
              if ((*it)[fieldId[i]] < minValue[i])
              {
                minValue[i] = (*it)[fieldId[i]];
              }
            }
          }
        }

        if (found == false)
        {
          return make_unique<NullQueryResult>();
        }
        return make_unique<SuccessMsgResult>(minValue);
      }
      else
      {
        return make_unique<NullQueryResult>();
      }
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
  catch (const IllFormedQueryCondition& e)
  {
    return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
  }
  catch (const invalid_argument& e)
  {
    // Cannot convert operand to string
    return make_unique<ErrorMsgResult>(qname, this->targetTable, "Unknown error '?'"_f % e.what());
  }
  catch (const exception& e)
  {
    return make_unique<ErrorMsgResult>(qname, this->targetTable, "Unkonwn error '?'."_f % e.what());
  }
}

std::string MinQuery::toString()
{
  return "QUERY = MIN " + this->targetTable + "\"";
}
