#include "DuplicateQuery.h"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "../../db/Database.h"
#include "../QueryResult.h"

constexpr const char* DuplicateQuery::qname;

QueryResult::Ptr DuplicateQuery::execute()
{
  using namespace std;
  try
  {
    if (!this->operands.empty())
    {
      return make_unique<ErrorMsgResult>(
          qname, this->targetTable, "Invalid number of operands (? operands)."_f % operands.size());
    }
    auto& db = Database::getInstance();
    auto& table = db[this->targetTable];
    auto result = initCondition(table);
    if (!result.second)
    {
      throw IllFormedQueryCondition("Error conditions in WHERE clause.");
    }

    // Collect keys to duplicate (store by value to avoid dangling pointers)
    std::vector<Table::KeyType> keysToDuplicate;

    for (auto it = table.begin(); it != table.end(); ++it)
    {
      if (!this->evalCondition(*it))
      {
        continue;
      }
      auto originalKey = it->key();
      // if a "_copy" already exists, skip this key
      if (table.evalDuplicateCopy(originalKey))
      {
        continue;
      }

      keysToDuplicate.push_back(originalKey);
    }

    for (const Table::KeyType& key : keysToDuplicate)
    {
      table.duplicateKeyData(key);
    }
    return make_unique<RecordCountResult>(keysToDuplicate.size());
  }
  catch (const NotFoundKey& e)
  {
    return make_unique<ErrorMsgResult>(qname, this->targetTable, "Key not found."s);
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

std::string DuplicateQuery::toString()
{
  return "QUERY = DUPLICATE " + this->targetTable + "\"";
}
