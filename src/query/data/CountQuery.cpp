#include "CountQuery.h"
#include "../../db/Database.h"

// Implementation of the execute method for CountQuery
QueryResult::Ptr CountQuery::execute() {
  // Use a try-catch block to handle potential exceptions gracefully
  try {
    // Get a reference to the database singleton instance
    Database &db = Database::getInstance();
    // Access the target table using the table name
    Table &table = db[this->targetTable];

    // Initialize the WHERE clause condition using the helper from the base class.
    // The 'second' member of the returned pair indicates if the condition can ever be true.
    auto condition = this->initCondition(table);
    
    // Initialize a counter for the records
    int record_count = 0;

    // Only proceed with iteration if the condition isn't always false
    if (condition.second) {
      // Iterate through each row (record) in the table
      for (const auto &row : table) {
        // Evaluate the WHERE clause for the current row
        if (this->evalCondition(row)) {
          // If the condition is met, increment the counter
          record_count++;
        }
      }
    }
    
    // According to p2.pdf, the output should be "ANSWER = <numRecords>"
    // The SuccessMsgResult(int) constructor formats the output correctly.
    return std::make_unique<SuccessMsgResult>(record_count);

  } catch (const TableNameNotFound &e) {
    // If the table does not exist, return an error message
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable, "Table not found.");
  } catch (const std::exception &e) {
    // Catch any other standard exceptions and return a generic error message
    return std::make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
  }
}

// Implementation of the toString method
std::string CountQuery::toString() {
  // Returns a string representation of the query, useful for debugging.
  return "QUERY = COUNT, Table = " + this->targetTable;
}