#include "DeleteQuery.h"

#include "../../db/Database.h"
#include "../QueryResult.h"

constexpr const char *DeleteQuery::qname;

QueryResult::Ptr DeleteQuery::execute()
{
    using namespace std;
    if (!this->operands.empty())
        return make_unique<ErrorMsgResult>(
            qname, this->targetTable.c_str(),
            "Invalid number of operands (? operands)."_f % operands.size());
    Database &db = Database::getInstance();
    Table::SizeType counter = 0;
    try
    {
        auto &table = db[this->targetTable];
        auto result = initCondition(table);
        if (result.second)
        {
            // for (Table::SizeType i = 0; i < table.size();)
            // {
            //     auto it = table.begin() + static_cast<int>(i); // avoid iterator invalidation
            //     if (this->evalCondition(*it))
            //     {
            //         auto toDeleteKey = it->key();
            //         table.deleteByIndex(toDeleteKey);
            //         ++counter;
            //     }
            //     else
            //     {
            //         ++i;
            //     }
            // }
            std::vector<Table::KeyType> keysToDelete;
            for (auto it = table.begin(); it != table.end(); it++)
            {
                if (this->evalCondition(*it))
                {
                    keysToDelete.push_back(it->key());
                    ++counter;
                }
            }
            for (const auto &key : keysToDelete)
            {
                table.deleteByIndex(key);
            }
        }
        else
        {
            throw IllFormedQueryCondition("Error conditions in WHERE clause.");
        }
        return std::make_unique<RecordCountResult>(counter);
    }
    catch (const NotFoundKey &e)
    {
        return make_unique<ErrorMsgResult>(qname, this->targetTable,
                                           "Key not found."s);
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

std::string DeleteQuery::toString()
{
    return "QUERY = DELETE " + this->targetTable + "\"";
}