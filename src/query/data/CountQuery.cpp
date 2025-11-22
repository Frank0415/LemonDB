#include "CountQuery.h"

#include <cstddef>
#include <exception>
#include <future>
#include <memory>
#include <string>
#include <vector>

#include "../../db/Database.h"
#include "../../db/Table.h"
#include "../../db/TableLockManager.h"
#include "../../threading/Threadpool.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"

// Implementation of the execute method for CountQuery
QueryResult::Ptr CountQuery::execute() {
  // Use a try-catch block to handle potential exceptions gracefully
  try {
    // Validate operands
    auto validation_result = validateOperands();
    if (validation_result != nullptr) [[unlikely]] {
      return validation_result;
    }

    // Get a reference to the database singleton instance
    Database &database = Database::getInstance();
    auto lock =
        TableLockManager::getInstance().acquireRead(this->targetTableRef());
    // Access the target table using the table name
    Table &table = database[this->targetTableRef()];

    // Initialize the WHERE clause condition. The 'second' member of the
    // returned pair is a flag indicating if the condition can ever be true.
    auto condition = this->initCondition(table);

    // An optimization: only proceed with iteration if the condition isn't
    // always false (e.g., WHERE (KEY = "a") (KEY = "b"))
    if (!condition.second) [[unlikely]] {
      return std::make_unique<TextRowsResult>("ANSWER = 0\n");
    }

    // Check if ThreadPool is available and has multiple threads
    if (!ThreadPool::isInitialized()) [[unlikely]] {
      return executeSingleThreaded(table);
    }

    const ThreadPool &pool = ThreadPool::getInstance();
    if (pool.getThreadCount() <= 1 || table.size() < Table::splitsize())
        [[unlikely]] {
      return executeSingleThreaded(table);
    }

    // Multi-threaded execution
    return executeMultiThreaded(table);
  } catch (const TableNameNotFound &e) {
    // If the specified table does not exist, return an error message
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            "Table not found.");
  } catch (const IllFormedQueryCondition &e) {
    // Handle errors in the WHERE clause, e.g., invalid field name
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            e.what());
  } catch (const std::exception &e) {
    // Catch any other standard exceptions and return a generic error message
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            e.what());
  }
}

// Implementation of the toString method
std::string CountQuery::toString() {
  // Returns a string representation of the query, useful for debugging.
  return "QUERY = COUNT, Table = " + this->targetTableRef();
}

[[nodiscard]] QueryResult::Ptr CountQuery::validateOperands() const {
  if (!this->getOperands().empty()) [[unlikely]] {
    return std::make_unique<ErrorMsgResult>(
        qname, this->targetTableRef(),
        "COUNT query does not take any operands.");
  }
  return nullptr;
}

[[nodiscard]] QueryResult::Ptr
CountQuery::executeSingleThreaded(const Table &table) {
  int record_count = 0;

  for (auto row : table) [[likely]]
  {
    if (this->evalCondition(row)) [[likely]] {
      record_count++;
    }
  }

  return std::make_unique<TextRowsResult>(
      "ANSWER = " + std::to_string(record_count) + "\n");
}

[[nodiscard]] QueryResult::Ptr
CountQuery::executeMultiThreaded(const Table &table) {
  constexpr size_t CHUNK_SIZE = Table::splitsize();
  const ThreadPool &pool = ThreadPool::getInstance();
  int total_count = 0;

  // Create chunks and submit tasks
  std::vector<std::future<int>> futures;
  futures.reserve((table.size() + CHUNK_SIZE - 1) / CHUNK_SIZE);

  auto iterator = table.begin();
  while (iterator != table.end()) [[likely]] {
    auto chunk_begin = iterator;
    size_t count = 0;
    while (iterator != table.end() && count < CHUNK_SIZE) [[likely]] {
      ++iterator;
      ++count;
    }
    auto chunk_end = iterator;

    futures.push_back(pool.submit([this, chunk_begin, chunk_end]() {
      int local_count = 0;
      for (auto iter = chunk_begin; iter != chunk_end; ++iter) [[likely]]
      {
        if (this->evalCondition(*iter)) [[likely]] {
          local_count++;
        }
      }
      return local_count;
    }));
  }

  // Combine results from all threads
  for (auto &future : futures) [[likely]]
  {
    total_count += future.get();
  }

  return std::make_unique<TextRowsResult>(
      "ANSWER = " + std::to_string(total_count) + "\n");
}
