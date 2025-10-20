#include "DeleteQuery.h"

#include "../../db/Database.h"
#include "../QueryResult.h"

constexpr const char *DeleteQuery::qname;

QueryResult::Ptr DeleteQuery::execute()
{
    using namespace std;
    if (this->operands.empty())
    {
        return make_unique<ErrorMsgResult>(qname, this->targetTable.c_str(),
                                           "No operand (? operands)."_f %
                                               operands.size());
    }
    Database &db = Database::getInstance();
    try
    {
        auto &table = db[this->targetTable];
        auto &key = this->operands.front();
        table.deleteByIndex(key);
        return std::make_unique<SuccessMsgResult>(qname, targetTable);
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