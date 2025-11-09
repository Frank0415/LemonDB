#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
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

namespace
{
std::string extractQueryString(std::istream& input_stream)
{
  std::string buf;
  while (true)
  {
    const int character = input_stream.get();
    if (character == ';') [[likely]]
    {
      return buf;
    }
    if (character == EOF) [[unlikely]]
    {
      throw std::ios_base::failure("End of input");
    }
    buf.push_back(static_cast<char>(character));
  }
}

void handleCopyTable(QueryManager& query_manager,
                     const std::string& trimmed,
                     const std::string& table_name,
                     CopyTableQuery* copy_query)
{
  if (copy_query != nullptr)
  {
    auto wait_sem = copy_query->getWaitSemaphore();
    constexpr size_t copytable_prefix_len = 9;
    auto new_table_name = trimmed.substr(copytable_prefix_len); // Skip "COPYTABLE"
    // Extract new table name - it's after first whitespace(s) and the source table
    size_t space_pos = new_table_name.find_first_not_of(" \t");
    if (space_pos != std::string::npos)
    {
      new_table_name = new_table_name.substr(space_pos);
      space_pos = new_table_name.find_first_of(" \t");
      if (space_pos != std::string::npos)
      {
        new_table_name = new_table_name.substr(space_pos);
        space_pos = new_table_name.find_first_not_of(" \t;");
        if (space_pos != std::string::npos)
        {
          new_table_name = new_table_name.substr(space_pos);
          space_pos = new_table_name.find_first_of(" \t;");
          if (space_pos != std::string::npos)
          {
            new_table_name = new_table_name.substr(0, space_pos);
          }
        }
      }
    }

    // Submit a WaitQuery to the new table's queue BEFORE the COPYTABLE query
    // NOTE: WaitQuery uses a special ID (0) since it's not a user query
    const size_t wait_query_id = 0; // Special ID for WaitQuery - not counted as user query
    auto wait_query = std::make_unique<WaitQuery>(table_name, wait_sem);
    query_manager.addQuery(wait_query_id, new_table_name, wait_query.release());
  }
}

void processQueries(std::istream& input_stream,
                    Database& database,
                    QueryParser& parser,
                    QueryManager& query_manager,
                    std::atomic<size_t>& g_query_counter)
{
  while (input_stream && !database.isEnd()) [[likely]]
  {
    try
    {
      const std::string queryStr = extractQueryString(input_stream);

      Query::Ptr query = parser.parseQuery(queryStr);

      // Use a case-insensitive check for "QUIT"
      std::string trimmed = queryStr;
      // Trim leading whitespace
      const size_t start = trimmed.find_first_not_of(" \t\n\r");
      if (start != std::string::npos)
      {
        trimmed = trimmed.substr(start);
      }

      if (query->isInstant() && trimmed.starts_with("QUIT"))
      {
        // Don't submit QUIT query - just break the loop
        break;
      }

      const size_t query_id = g_query_counter.fetch_add(1) + 1;

      // Get the target table for this query
      const std::string table_name = query->targetTableRef();

      // Handle COPYTABLE specially: add WaitQuery to the new table's queue
      if (trimmed.starts_with("COPYTABLE"))
      {
        auto* copy_query = dynamic_cast<CopyTableQuery*>(query.get());
        handleCopyTable(query_manager, trimmed, table_name, copy_query);
      }

      // Submit query to manager (async - doesn't block)
      // Manager will create per-table thread if needed
      query_manager.addQuery(query_id, table_name, query.release());
    }
    catch (const std::ios_base::failure& exc)
    {
      break;
    }
    catch (const std::exception& exc)
    {
      (void)exc;
    }
  }
}

std::optional<size_t> setupListenMode(const Args& args,
                                      QueryParser& parser,
                                      Database& database,
                                      QueryManager& query_manager,
                                      std::atomic<size_t>& g_query_counter)
{
  if (args.listen.empty())
  {
    return std::nullopt;
  }

  auto listen_query = std::make_unique<ListenQuery>(args.listen);
  listen_query->setDependencies(&query_manager, &parser, &database, &g_query_counter);

  auto listen_result = listen_query->execute();
  if (listen_result && listen_result->display())
  {
    std::cout << *listen_result;
  }

  return listen_query->getScheduledQueryCount();
}
} // namespace

int main(int argc, char* argv[])
{
  std::ios_base::sync_with_stdio(true);
  std::ios_base::sync_with_stdio(true);

  Args parsedArgs{};
  MainUtils::parseArgs(argc, argv, parsedArgs);

  std::ifstream fin;
  std::istream* input = &std::cin;
  if (!parsedArgs.listen.empty()) [[unlikely]]
  {
    fin = std::ifstream(parsedArgs.listen);
    if (!fin.is_open()) [[unlikely]]
    {
      std::cerr << "lemondb: error: " << parsedArgs.listen << ": no such file or directory" << '\n';
      std::exit(-1);
    }
    input = &fin;
  }

  ThreadPool::initialize(parsedArgs.threads > 0 ? static_cast<size_t>(parsedArgs.threads)
                                                : std::thread::hardware_concurrency());

#ifdef NDEBUG
  // In production mode, listen argument must be defined
  if (parsedArgs.listen.empty())
  {
    std::cerr << "lemondb: error: --listen argument not found, not allowed in "
                 "production mode"
              << '\n';
    exit(-1);
  }
#else
  // In debug mode, use stdin as input if no listen file is found
  if (parsedArgs.listen.empty())
  {
    std::cerr << "lemondb: warning: --listen argument not found, use stdin "
                 "instead in debug mode\n";
    input = &std::cin;
  }
#endif

  std::istream& input_stream = *input;

  QueryParser parser;
  MainUtils::setupParser(parser);

  // Main loop: ASYNC query submission with table-level parallelism
  Database& database = Database::getInstance();

  // Create OutputPool (not a global singleton - passed by reference)
  OutputPool output_pool;

  // Create QueryManager with reference to OutputPool
  QueryManager query_manager(output_pool);

  std::atomic<size_t> g_query_counter{0};

  const auto listen_scheduled =
      setupListenMode(parsedArgs, parser, database, query_manager, g_query_counter);
  if (!listen_scheduled.has_value())
  {
    processQueries(input_stream, database, parser, query_manager, g_query_counter);
  }

  size_t total_queries = g_query_counter.load();
  try
  {
    if (listen_scheduled.has_value())
    {
      total_queries = listen_scheduled.value();
    }
  }
  catch (const std::bad_optional_access& e)
  {
    std::cerr << "Error accessing listen_scheduled: " << e.what() << "\n";
    return -1;
  }
  query_manager.setExpectedQueryCount(total_queries);

  const OutputConfig output_config{};
  while (true)
  {
    const size_t current_output_count = output_pool.getTotalOutputCount();
    const auto interval = calculateOutputInterval(current_output_count, output_config);

    const size_t flushed = output_pool.flushContinuousResults();
    if (query_manager.isComplete())
    {
      if (flushed == 0U)
      {
        break;
      }
    }
    else if (flushed == 0U)
    {
      std::this_thread::sleep_for(interval);
    }
  }

  query_manager.waitForCompletion();
  output_pool.outputAllResults();

  return 0;
}

#ifdef LEMONDB_WITH_MSAN
#undef LEMONDB_WITH_MSAN
#endif
