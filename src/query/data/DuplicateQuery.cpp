#include <sstream>
#include <algorithm>
#include "DuplicateQuery.h"
#include "../../db/Database.h"
#include "../QueryResult.h"

constexpr const char *DuplicateQuery::qname;

QueryResult::Ptr DuplicateQuery::execute()
{
    using namespace std;
    try
    {
        if (!this->operands.empty())
        {
            return make_unique<ErrorMsgResult>(
                qname, this->targetTable,
                "Invalid number of operands (? operands)."_f % operands.size());
        }
        auto &db = Database::getInstance();
        auto &table = db[this->targetTable];
        auto result = initCondition(table);
        if (!result.second)
        {
            throw IllFormedQueryCondition("Error conditions in WHERE clause.");
        }

        std::vector<Table::KeyType> keysToDuplicate;
        for (auto it = table.begin(); it != table.end(); ++it)
        {
            if (this->evalCondition(*it))
            {
                auto originalKey = it->key();
                auto copyKey = originalKey + "_copy";

                auto existingCopy = table[copyKey];
                if (existingCopy == nullptr)
                {
                    keysToDuplicate.push_back(originalKey);
                }
            }
        }
    }
}

std::string DuplicateQuery::toString()
{
    return "QUERY = DUPLICATE " + this->targetTable + "\"";
}