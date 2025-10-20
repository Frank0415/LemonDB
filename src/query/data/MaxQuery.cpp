#include "MaxQuery.h"

#include "../../db/Database.h"
#include "../QueryResult.h"

constexpr const char *MaxQuery::qname;

QueryResult::Ptr MaxQuery::execute()
{
    using namespace std;
    if (this->operands.empty())
    {
        return make_unique<ErrorMsgResult>(
            qname, this->targetTable.c_str(),
            "No operand (? operands)."_f % operands.size());
    }

    Database &db = Database::getInstance();
    try
    {
        auto &table = db[this->targetTable];
        auto result = initCondition(table);
        if (result.second)
        {
            bool found = false;
            vector<Table::ValueType> maxValue; // each has its own max value
            for (size_t i = 0; i < this->operands.size(); i++)
            {
                maxValue.push_back(Table::ValueTypeMin);
            }
            string resultStr = "MAX RESULT : ( ";

            for (auto it = table.begin(); it != table.end(); ++it)
            {
                if (this->evalCondition(*it))
                {
                    for (size_t i = 0; i < maxValue.size(); ++i)
                    {
                        if ((*it)[table.getFieldIndex(this->operands[i])] >
                            maxValue[i])
                        {
                            found = true;
                            maxValue[i] = (*it)[table.getFieldIndex(this->operands[i])];
                        }
                    }
                }
            }

            for (size_t i = 0; i < maxValue.size(); i++)
            {
                resultStr += to_string(maxValue[i]) + " ";
            }

            resultStr += ")";
            if (found == false)
            {
            }
            return make_unique<SuccessMsgResult>(qname, resultStr);
        }
        else
        {
            return make_unique<NullQueryResult>();
        }
    }
    catch (const TableNameNotFound &e)
    {
        return make_unique<ErrorMsgResult>(qname, this->targetTable,
                                           "No such table."s);
    }
    catch (const IllFormedQueryCondition &e)
    {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
    }
    catch (const invalid_argument &e)
    {
        // Cannot convert operand to string
        return make_unique<ErrorMsgResult>(qname, this->targetTable,
                                           "Unknown error '?'"_f % e.what());
    }
    catch (const exception &e)
    {
        return make_unique<ErrorMsgResult>(qname, this->targetTable,
                                           "Unkonwn error '?'."_f % e.what());
    }
}

std::string MaxQuery::toString()
{
    return "QUERY = MAX " + this->targetTable + "\"";
}