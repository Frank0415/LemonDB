#include "MainQueryHelpers.h"

#include <atomic>
#include <cstddef>
#include <cstdio>
#include <deque>
#include <exception>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <utility>

#include "../db/Database.h"
#include "../db/QueryBase.h"
#include "../query/QueryParser.h"
#include "../query/management/CopyTableQuery.h"
#include "../query/management/WaitQuery.h"
#include "../query/utils/ListenQuery.h"
#include "../threading/QueryManager.h"
#include "MainUtils.h"

namespace MainQueryHelpers {
std::string extractQueryString(std::istream &input_stream) {
  std::string buf;
  while (true) {
    const int character = input_stream.get();
    if (character == ';') [[likely]] {
      return buf;
    }
    if (character == EOF) [[unlikely]] {
      throw std::ios_base::failure("End of input");
    }
    buf.push_back(static_cast<char>(character));
  }
}

std::string trimLeadingWhitespace(const std::string &str) {
  const size_t start = str.find_first_not_of(" \t\n\r");
  if (start == std::string::npos) {
    return str;
  }
  return str.substr(start);
}

void handleListenQuery(ListenQuery *listen_query, QueryManager &query_manager,
                       std::atomic<size_t> &g_query_counter,
                       QueryParser &parser, Database &database,
                       bool &should_break) {
  std::deque<std::unique_ptr<ListenQuery>> pending_listens;

  listen_query->setDependencies(&query_manager, &parser, &database,
                                &g_query_counter, &pending_listens);

  size_t listen_query_id = listen_query->getId();
  if (listen_query_id == 0) {
    listen_query_id = g_query_counter.fetch_add(1) + 1;
  }
  // std::cerr << "[CONTROL] Handing control to LISTEN file " <<
  // listen_query->getFileName()
  //           << '\n';
  auto result = listen_query->execute();
  if (result != nullptr) {
    const bool should_display = result->display();
    if (should_display) {
      std::ostringstream result_stream;
      result_stream << *result;
      query_manager.addImmediateResult(listen_query_id, result_stream.str());
    }
  }
  // std::cerr << "[CONTROL] Returned from LISTEN file " <<
  // listen_query->getFileName()
  //           << '\n';

  if (listen_query->hasEncounteredQuit()) {
    should_break = true;
    return;
  }

  while (!pending_listens.empty()) {
    auto next_listen = std::move(pending_listens.front());
    pending_listens.pop_front();

    next_listen->setDependencies(&query_manager, &parser, &database,
                                 &g_query_counter, &pending_listens);

    size_t next_id = next_listen->getId();
    if (next_id == 0) {
      next_id = g_query_counter.fetch_add(1) + 1;
    }
    auto next_result = next_listen->execute();
    if (next_result && next_result->display()) {
      std::ostringstream oss;
      oss << *next_result;
      query_manager.addImmediateResult(next_id, oss.str());
    }

    if (next_listen->hasEncounteredQuit()) {
      should_break = true;
      return;
    }
  }
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
void handleCopyTable(QueryManager &query_manager, const std::string &trimmed,
                     const std::string &table_name,
                     CopyTableQuery *copy_query) {
  if (copy_query != nullptr) {
    auto wait_sem = copy_query->getWaitSemaphore();
    constexpr size_t copytable_prefix_len = 9;
    auto new_table_name =
        trimmed.substr(copytable_prefix_len);  // Skip "COPYTABLE"
    // Extract new table name - it's after first whitespace(s) and the source
    // table
    size_t space_pos = new_table_name.find_first_not_of(" \t");
    if (space_pos != std::string::npos) {
      new_table_name = new_table_name.substr(space_pos);
      space_pos = new_table_name.find_first_of(" \t");
      if (space_pos != std::string::npos) {
        new_table_name = new_table_name.substr(space_pos);
        space_pos = new_table_name.find_first_not_of(" \t;");
        if (space_pos != std::string::npos) {
          new_table_name = new_table_name.substr(space_pos);
          space_pos = new_table_name.find_first_of(" \t;");
          if (space_pos != std::string::npos) {
            new_table_name = new_table_name.substr(0, space_pos);
          }
        }
      }
    }

    // Submit a WaitQuery to the new table's queue BEFORE the COPYTABLE query
    // NOTE: WaitQuery uses a special ID (0) since it's not a user query
    const size_t wait_query_id =
        0;  // Special ID for WaitQuery - not counted as user query
    auto wait_query = std::make_unique<WaitQuery>(table_name, wait_sem);
    query_manager.addQuery(wait_query_id, new_table_name, wait_query.release());
  }
}

void processQueries(std::istream &input_stream, Database &database,
                    QueryParser &parser, QueryManager &query_manager,
                    std::atomic<size_t> &g_query_counter) {
  while (input_stream && !database.isEnd()) [[likely]] {
    try {
      const std::string queryStr = extractQueryString(input_stream);

      // std::cerr << "[CONTROL] Read query: " << queryStr << '\n';

      Query::Ptr query = parser.parseQuery(queryStr);

      // Use a case-insensitive check for "QUIT"
      const std::string trimmed = trimLeadingWhitespace(queryStr);

      if (query->isInstant() && trimmed.starts_with("QUIT")) {
        // Don't submit QUIT query - just break the loop
        break;
      }

      // Get the target table for this query
      const std::string table_name = query->targetTableRef();

      // Handle LISTEN queries eagerly so they are not scheduled again
      if (trimmed.starts_with("LISTEN")) {
        auto *listen_query = dynamic_cast<ListenQuery *>(query.get());
        if (listen_query != nullptr) {
          bool should_break = false;
          handleListenQuery(listen_query, query_manager, g_query_counter,
                            parser, database, should_break);
          if (should_break) {
            break;
          }

          continue;
        }
      }

      const size_t query_id = g_query_counter.fetch_add(1) + 1;

      // Handle COPYTABLE specially: add WaitQuery to the new table's queue
      if (trimmed.starts_with("COPYTABLE")) {
        auto *copy_query = dynamic_cast<CopyTableQuery *>(query.get());
        handleCopyTable(query_manager, trimmed, table_name, copy_query);
      }

      // Submit query to manager (async - doesn't block)
      // Manager will create per-table thread if needed
      query_manager.addQuery(query_id, table_name, query.release());
    } catch (const std::ios_base::failure &exc) {
      // std::cerr << "[CONTROL] Input error: " << exc.what() << '\n';
      break;
    } catch (const std::exception &exc) {
      // std::cerr << "[CONTROL] Query processing error: " << exc.what() <<
      // '\n';
      (void)exc;
    }
  }
}

std::optional<size_t> setupListenMode(const Args &args, QueryParser &parser,
                                      Database &database,
                                      QueryManager &query_manager,
                                      std::atomic<size_t> &g_query_counter) {
  if (args.listen.empty()) {
    return std::nullopt;
  }

  std::deque<std::unique_ptr<ListenQuery>> pending_listens;
  pending_listens.push_back(std::make_unique<ListenQuery>(args.listen));

  size_t total_scheduled = 0;
  bool is_root_listen = true;

  while (!pending_listens.empty()) {
    auto listen_query = std::move(pending_listens.front());
    pending_listens.pop_front();

    listen_query->setDependencies(&query_manager, &parser, &database,
                                  &g_query_counter, &pending_listens);

    if (is_root_listen) {
      listen_query->execute();
      total_scheduled += listen_query->getScheduledQueryCount();
      is_root_listen = false;
    } else {
      size_t listen_query_id = listen_query->getId();
      if (listen_query_id == 0) {
        listen_query_id = g_query_counter.fetch_add(1) + 1;
      }

      auto listen_result = listen_query->execute();
      if (listen_result != nullptr) {
        const bool should_display = listen_result->display();
        if (should_display) {
          std::ostringstream result_stream;
          result_stream << *listen_result;
          query_manager.addImmediateResult(listen_query_id,
                                           result_stream.str());
        }
      }

      total_scheduled += listen_query->getScheduledQueryCount() + 1;
    }

    if (listen_query->hasEncounteredQuit()) {
      break;
    }
  }

  // std::cerr << "Scheduled " << total_scheduled
  //           << " queries from listen file.\n";
  return total_scheduled;
}

size_t
determineExpectedQueryCount(const std::optional<size_t> &listen_scheduled,
                            const std::atomic<size_t> &g_query_counter) {
  if (listen_scheduled.has_value()) {
    return listen_scheduled.value();
  }
  return g_query_counter.load();
}
}  // namespace MainQueryHelpers
