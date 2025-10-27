#include "CopyTableQuery.h"

#include <cstddef>
#include <exception>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../../db/Database.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"

constexpr const char* CopyTableQuery::qname;

QueryResult::Ptr CopyTableQuery::execute()
{
  try
  {
    auto& db = Database::getInstance();
    auto& src = db[this->targetTable];
    bool targetExists = false;
    try
    {
      (void)db[this->newTableName];
      targetExists = true;
    }
    catch (...)
    {
      // Intentionally ignore: table doesn't exist, which is expected
      (void)0;
    }
    if (targetExists)
    {
      return std::make_unique<ErrorMsgResult>(qname, this->targetTable, "Target table name exists");
    }

    std::vector<std::string> fields = src.field();
    auto dup = std::make_unique<Table>(this->newTableName, fields);
    for (auto it = src.begin(); it != src.end(); ++it)
    {
      const auto& obj = *it;
      std::vector<Table::ValueType> row;
      row.reserve(fields.size());
      for (size_t i = 0; i < fields.size(); ++i)
      {
        row.push_back(obj[i]);
      }
      dup->insertByIndex(obj.key(), std::move(row));
    }

    db.registerTable(std::move(dup));
    return std::make_unique<NullQueryResult>();
  }
  catch (const TableNameNotFound&)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable, "No such table.");
  }
  catch (const std::exception& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable, "Unknown error");
  }
}

std::string CopyTableQuery::toString()
{
  return "QUERY = COPYTABLE, SOURCE = \"" + this->targetTable + "\", TARGET = \"" +
         this->newTableName + "\"";
}
