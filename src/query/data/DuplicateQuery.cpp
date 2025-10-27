#include "DuplicateQuery.h"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
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

    Table::SizeType counter = 0;
    auto row_size = table.field().size();

    // Collect records to duplicate
    std::vector<std::pair<Table::KeyType, std::vector<Table::ValueType>>> recordsToDuplicate;

    for (auto it = table.begin(); it != table.end(); ++it)
    {
      if (!this->evalCondition(*it))
      {
        continue;
      }
      auto originalKey = it->key();
      auto newKey = originalKey + "_copy";

      // if a "_copy" already exists, skip this key
      if (table[newKey] != nullptr)
      {
        continue;
      }

      // Copy the values from the original record
      std::vector<Table::ValueType> values(row_size);
      for (size_t i = 0; i < row_size; ++i)
      {
        values[i] = (*it)[i];
      }

      recordsToDuplicate.emplace_back(newKey, std::move(values));
    }

    // Insert all duplicated records
    for (auto& record : recordsToDuplicate)
    {
      try
      {
        table.insertByIndex(record.first, std::move(record.second));
        ++counter;
      }
      catch (const ConflictingKey& e)
      {
        // if key conflict exists, skip
      }
    }

    return make_unique<RecordCountResult>(counter);
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
