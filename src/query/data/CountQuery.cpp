#include "CountQuery.h"

#include <exception>
#include <future>
#include <memory>
#include <string>

#include "../../db/Database.h"
#include "../../db/Table.h"
#include "../../db/TableLockManager.h"
#include "../../threading/Threadpool.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"

// Implementation of the execute method for CountQuery
QueryResult::Ptr CountQuery::execute()
{
  // Use a try-catch block to handle potential exceptions gracefully
  try
  {
    // According to the PDF, COUNT should not have any operands
    if (!this->operands.empty())
    {
      return std::make_unique<ErrorMsgResult>(qname, this->targetTable,
                                              "COUNT query does not take any operands.");
    }

    // Get a reference to the database singleton instance
    Database& database = Database::getInstance();
    auto lock = TableLockManager::getInstance().acquireRead(this->targetTable);
    // Access the target table using the table name
    Table& table = database[this->targetTable];

    // Initialize the WHERE clause condition. The 'second' member of the
    // returned pair is a flag indicating if the condition can ever be true.
    auto condition = this->initCondition(table);

    // Initialize a counter for the records that match the condition
    int record_count = 0;

    // An optimization: only proceed with iteration if the condition isn't
    // always false (e.g., WHERE (KEY = "a") (KEY = "b"))
    if (condition.second)
    {
      // Iterate through each row (record) in the table
      for (const auto& row : table)
      {
        // Evaluate the WHERE clause for the current row.
        // If there's no WHERE clause, evalCondition returns true.
        if (this->evalCondition(row))
        {
          // If the condition is met, increment the counter
          record_count++;
        }
      }
    }

    // According to p2.pdf, the output format is "ANSWER = <numRecords>".
    // We use TextRowsResult to format this string and ensure it's printed to
    // standard output, as its display() method returns true.
    return std::make_unique<TextRowsResult>("ANSWER = " + std::to_string(record_count) + "\n");
  }
  catch (const TableNameNotFound& e)
  {
    // If the specified table does not exist, return an error message
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable, "Table not found.");
  }
  catch (const IllFormedQueryCondition& e)
  {
    // Handle errors in the WHERE clause, e.g., invalid field name
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
  }
  catch (const std::exception& e)
  {
    // Catch any other standard exceptions and return a generic error message
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
  }
}

// Implementation of the toString method
std::string CountQuery::toString()
{
  // Returns a string representation of the query, useful for debugging.
  return "QUERY = COUNT, Table = " + this->targetTable;
}

[[nodiscard]] QueryResult::Ptr CountQuery::validateOperands() const
{
  if (!this->operands.empty())
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable,
                                            "COUNT query does not take any operands.");
  }
  return nullptr;
}

[[nodiscard]] QueryResult::Ptr CountQuery::executeSingleThreaded(Table& table)
{
  int record_count = 0;

  for (auto it = table.begin(); it != table.end(); ++it)
  {
    if (this->evalCondition(*it))
    {
      record_count++;
    }
  }

  return std::make_unique<TextRowsResult>("ANSWER = " + std::to_string(record_count) + "\n");
}
