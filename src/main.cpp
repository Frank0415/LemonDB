#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <queue>
#include <sstream>
#include <string>
#include <thread>

#include "db/Database.h"
#include "db/QueryBase.h"
#include "query/QueryParser.h"
#include "query/management/CopyTableQuery.h"
#include "query/management/WaitQuery.h"
#include "query/utils/ListenQuery.h"
#include "threading/OutputPool.h"
#include "threading/QueryManager.h"
#include "threading/Threadpool.h"
#include "utils/MainUtils.h"
#include "utils/OutputConfig.h"

namespace {
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
      std::string trimmed = trimLeadingWhitespace(queryStr);

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

  while (!pending_listens.empty()) {
    auto listen_query = std::move(pending_listens.front());
    pending_listens.pop_front();

    listen_query->setDependencies(&query_manager, &parser, &database,
                                  &g_query_counter, &pending_listens);

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
        query_manager.addImmediateResult(listen_query_id, result_stream.str());
      }
    }

    total_scheduled += listen_query->getScheduledQueryCount() + 1;

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

void flushOutputLoop(OutputPool &output_pool, QueryManager &query_manager,
                     const OutputConfig &output_config) {
  while (true) {
    const size_t current_output_count = output_pool.getTotalOutputCount();
    const auto interval =
        calculateOutputInterval(current_output_count, output_config);

    const size_t flushed = output_pool.flushContinuousResults();
    if (query_manager.isComplete()) {
      if (flushed == 0U) {
        break;
      }
    } else if (flushed == 0U) {
      std::this_thread::sleep_for(interval);
    }
  }
}

std::istream *initializeInputStream(const Args &parsedArgs,
                                    std::ifstream &fin) {
  if (!parsedArgs.listen.empty()) [[unlikely]] {
    fin = std::ifstream(parsedArgs.listen);
    if (!fin.is_open()) [[unlikely]] {
      std::cerr << "lemondb: error: " << parsedArgs.listen
                << ": no such file or directory" << '\n';
      std::exit(-1);
    }
    return &fin;
  }
  return &std::cin;
}

void validateProductionMode(const Args &parsedArgs) {
#ifdef NDEBUG
  // In production mode, listen argument must be defined
  if (parsedArgs.listen.empty()) {
    std::cerr << "lemondb: error: --listen argument not found, not allowed in "
                 "production mode"
              << '\n';
    exit(-1);
  }
#else
  // In debug mode, use stdin as input if no listen file is found
  if (parsedArgs.listen.empty()) {
    std::cerr << "lemondb: warning: --listen argument not found, use stdin "
                 "instead in debug mode\n";
  }
#endif
}
}  // namespace

int main(int argc, char *argv[]) {
  std::ios_base::sync_with_stdio(true);
  std::ios_base::sync_with_stdio(true);

  Args parsedArgs{};
  MainUtils::parseArgs(argc, argv, parsedArgs);

  std::ifstream fin;
  std::istream *input = initializeInputStream(parsedArgs, fin);

  ThreadPool::initialize(parsedArgs.threads > 0
                             ? static_cast<size_t>(parsedArgs.threads)
                             : std::thread::hardware_concurrency());

  validateProductionMode(parsedArgs);

  std::istream &input_stream = *input;

  QueryParser parser;
  MainUtils::setupParser(parser);

  // Main loop: ASYNC query submission with table-level parallelism
  Database &database = Database::getInstance();

  // Create OutputPool (not a global singleton - passed by reference)
  OutputPool output_pool;

  // Create QueryManager with reference to OutputPool
  QueryManager query_manager(output_pool);

  std::atomic<size_t> g_query_counter{0};

  const auto listen_scheduled = setupListenMode(parsedArgs, parser, database,
                                                query_manager, g_query_counter);
  if (!listen_scheduled.has_value()) {
    processQueries(input_stream, database, parser, query_manager,
                   g_query_counter);
  }

  const size_t total_queries =
      determineExpectedQueryCount(listen_scheduled, g_query_counter);
  query_manager.setExpectedQueryCount(total_queries);

  const OutputConfig output_config{};
  flushOutputLoop(output_pool, query_manager, output_config);

  query_manager.waitForCompletion();
  output_pool.outputAllResults();

  return 0;
}

#ifdef LEMONDB_WITH_MSAN
#undef LEMONDB_WITH_MSAN
#endif
