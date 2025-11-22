#include "ListenQuery.h"

#include <atomic>
#include <cctype>
#include <cstddef>
#include <cstdio>
#include <deque>
#include <exception>
#include <fstream>
#include <ios>
#include <iostream>
#include <memory>
#include <sstream>
#include <stack>
#include <stdexcept>
#include <string>
#include <string_view>

#include "db/Database.h"
#include "query/QueryParser.h"
#include "query/QueryResult.h"
#include "query/management/CopyTableQuery.h"
#include "query/management/WaitQuery.h"
#include "threading/QueryManager.h"
#include "utils/formatter.h"

namespace {
std::string trimCopy(std::string_view input) {
  const auto first = input.find_first_not_of(" \t\n\r");
  if (first == std::string_view::npos) {
    return "";
  }
  const auto last = input.find_last_not_of(" \t\n\r");
  return std::string(input.substr(first, last - first + 1));
}

bool startsWithCaseInsensitive(std::string_view value,
                               std::string_view prefix) {
  if (value.size() < prefix.size()) {
    return false;
  }
  for (size_t index = 0; index < prefix.size(); ++index) {
    const auto lhs = static_cast<unsigned char>(value[index]);
    const auto rhs = static_cast<unsigned char>(prefix[index]);
    if (std::toupper(lhs) != std::toupper(rhs)) {
      return false;
    }
  }
  return true;
}

bool readNextStatement(std::istream &stream, std::string &out_statement) {
  out_statement.clear();
  while (true) {
    const int character = stream.get();
    if (character == ';') {
      return true;
    }
    if (character == EOF) {
      if (out_statement.empty()) {
        return false;
      }
      throw std::ios_base::failure("Unexpected end of input before ';'");
    }
    out_statement.push_back(static_cast<char>(character));
  }
}

std::string extractNewTableName(const std::string &trimmed) {
  constexpr size_t copytable_prefix_len = 9;  // length of "COPYTABLE"
  std::string new_table_name = trimmed.substr(copytable_prefix_len);

  size_t position = new_table_name.find_first_not_of(" \t");
  if (position == std::string::npos) {
    return "";
  }
  new_table_name = new_table_name.substr(position);

  position = new_table_name.find_first_of(" \t");
  if (position == std::string::npos) {
    return "";
  }
  new_table_name = new_table_name.substr(position);

  position = new_table_name.find_first_not_of(" \t;");
  if (position == std::string::npos) {
    return "";
  }
  new_table_name = new_table_name.substr(position);

  position = new_table_name.find_first_of(" \t;");
  if (position != std::string::npos) {
    new_table_name = new_table_name.substr(0, position);
  }

  return new_table_name;
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
void handleCopyTable(QueryManager &query_manager, const std::string &trimmed,
                     const std::string &source_table,
                     CopyTableQuery *copy_query) {
  if (copy_query == nullptr) {
    return;
  }

  const std::string new_table_name = extractNewTableName(trimmed);
  if (new_table_name.empty()) {
    return;
  }

  auto wait_query =
      std::make_unique<WaitQuery>(source_table, copy_query->getWaitSemaphore());
  constexpr size_t wait_query_id = 0;
  query_manager.addQuery(wait_query_id, new_table_name, wait_query.release());
}
}  // namespace

void ListenQuery::setDependencies(
    QueryManager *manager, QueryParser *parser, Database *database_ptr,
    std::atomic<size_t> *counter,
    std::deque<std::unique_ptr<ListenQuery>> *pending_queue) {
  query_manager = manager;
  query_parser = parser;
  database = database_ptr;
  query_counter = counter;
  pending_listens = pending_queue;
}

bool ListenQuery::shouldSkipStatement(const std::string &trimmed) {
  return trimmed.empty() || trimmed.front() == '#';
}

bool ListenQuery::processStatement(const std::string &trimmed,
                                   std::string *nested_file_out) {
  Query::Ptr query = query_parser->parseQuery(trimmed);
  // std::cerr << "[LISTEN] Parsed query: " << query->toString() << '\n';

  if (startsWithCaseInsensitive(trimmed, "QUIT")) {
    // std::cerr << "[LISTEN] Found QUIT in listen file, stopping" << '\n';
    database->exit();
    quit_encountered = true;
    return false;  // Stop processing
  }

  if (startsWithCaseInsensitive(trimmed, "COPYTABLE")) {
    handleCopyTable(*query_manager, trimmed, query->targetTableRef(),
                    dynamic_cast<CopyTableQuery *>(query.get()));
  }

  if (auto *nested_listen = dynamic_cast<ListenQuery *>(query.get())) {
    if (pending_listens != nullptr) {
      (void)query.release();  // NOLINT
      // Assign ID immediately to preserve order
      const size_t nested_id = query_counter->fetch_add(1) + 1;
      nested_listen->setId(nested_id);
      pending_listens->push_back(std::unique_ptr<ListenQuery>(nested_listen));
      // Increment scheduled count because this nested listen is a query
      // scheduled by us
      scheduled_query_count++;
      return true;
    }

    if (nested_file_out != nullptr) {
      *nested_file_out = nested_listen->getFileName();
    }
    return true;
  }

  const size_t query_id = query_counter->fetch_add(1) + 1;
  // std::cerr << "[LISTEN] Adding query " << query_id << " to table " <<
  // query->targetTableRef() << '\n';
  query_manager->addQuery(query_id, query->targetTableRef(), query.release());
  scheduled_query_count++;
  // std::cerr << "[LISTEN] Scheduled query count: " << scheduled_query_count <<
  // '\n';

  return true;  // Continue processing
}

QueryResult::Ptr ListenQuery::execute() {
  if (query_manager == nullptr || query_parser == nullptr ||
      database == nullptr || query_counter == nullptr) {
    throw std::runtime_error("ListenQuery dependencies are not set");
  }

  struct FileContext {
    std::string name;
    std::unique_ptr<std::ifstream> stream;
  };

  std::stack<FileContext> file_stack;
  auto initial_stream = std::make_unique<std::ifstream>(fileName);
  if (!initial_stream->is_open()) {
    return std::make_unique<ErrorMsgResult>(qname, "Cannot open file '?'"_f %
                                                       fileName);
  }
  file_stack.push({fileName, std::move(initial_stream)});

  scheduled_query_count = 0;
  quit_encountered = false;
  std::string raw_statement;

  while (!file_stack.empty()) {
    auto &current_ctx = file_stack.top();

    try {
      if (!readNextStatement(*current_ctx.stream, raw_statement)) {
        if (file_stack.size() > 1) {
          std::ostringstream oss;
          oss << ListenResult(current_ctx.name);
          query_manager->addImmediateResult(query_counter->fetch_add(1) + 1,
                                            oss.str());
        }
        file_stack.pop();
        continue;
      }

      const std::string trimmed = trimCopy(raw_statement);
      if (shouldSkipStatement(trimmed)) {
        continue;
      }

      std::string nested_file;
      try {
        if (!processStatement(trimmed, &nested_file)) {
          break;  // QUIT encountered
        }
      } catch (const std::exception & /*ignored*/) {
        continue;
      }

      if (!nested_file.empty()) {
        auto nested_stream = std::make_unique<std::ifstream>(nested_file);
        if (!nested_stream->is_open()) {
          std::ostringstream oss;
          oss << ErrorMsgResult(qname, "Cannot open file '?'"_f % nested_file);
          query_manager->addImmediateResult(query_counter->fetch_add(1) + 1,
                                            oss.str());
        } else {
          file_stack.push({nested_file, std::move(nested_stream)});
        }
      }
    } catch (const std::ios_base::failure &) {
      if (file_stack.size() == 1) {
        return std::make_unique<ErrorMsgResult>(
            qname, "Unexpected EOF in listen file '?'"_f % current_ctx.name);
      }
      std::ostringstream oss;
      oss << ErrorMsgResult(qname, "Unexpected EOF in listen file '?'"_f %
                                       current_ctx.name);
      query_manager->addImmediateResult(query_counter->fetch_add(1) + 1,
                                        oss.str());
      file_stack.pop();
    }
  }

  if (!quit_encountered) {
    // std::cerr << "[LISTEN] Reached end of file for " << fileName << '\n';
  }

  return std::make_unique<ListenResult>(fileName);
}

std::string ListenQuery::toString() {
  return "QUERY = Listen, FILE = \"" + fileName + "\"";
}
