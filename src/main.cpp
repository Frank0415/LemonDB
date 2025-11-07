//
// Created by liu on 18-10-21.
//

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <fstream>
#include <iostream>
#include <span>
#include <string>
#include <thread>
#include <typeinfo>

#include "db/Database.h"
#include "db/QueryBase.h"
#include "query/QueryBuilders.h"
#include "query/QueryParser.h"
#include "query/management/CopyTableQuery.h"
#include "query/management/WaitQuery.h"
#include "threading/OutputPool.h"
#include "threading/QueryManager.h"
#include "threading/Threadpool.h"

namespace
{
struct Args
{
  std::string listen;
  std::int64_t threads = 0;
};

void parseArgs(int argc, char** argv, Args& args)
{
  // Manual argument parser supporting both long and short forms
  // --listen=<file> or --listen <file> or -l <file>
  // --threads=<num> or --threads <num> or -t <num>

  constexpr size_t listen_prefix_len = 9;   // Length of "--listen="
  constexpr size_t threads_prefix_len = 10; // Length of "--threads="
  constexpr int decimal_base = 10;

  for (int i = 1; i < argc; ++i)
  {
    const std::string arg(
        std::span(argv, static_cast<std::size_t>(argc))[static_cast<std::size_t>(i)]);

    // Helper lambda to get next argument value
    auto getNextArg = [&]() -> std::string
    {
      if (i + 1 < argc)
      {
        ++i;
        return {std::span(argv, static_cast<std::size_t>(argc))[static_cast<std::size_t>(i)]};
      }
      std::cerr << "lemondb: error: " << arg << " requires an argument\n";
      std::exit(-1);
    };

    // Handle --listen=<value> or --listen <value>
    if (arg.starts_with("--listen="))
    {
      args.listen = arg.substr(listen_prefix_len);
    }
    else if (arg == "--listen" || arg == "-l")
    {
      args.listen = getNextArg();
    }
    // Handle --threads=<value> or --threads <value>
    else if (arg.starts_with("--threads="))
    {
      args.threads = std::strtol(arg.substr(threads_prefix_len).c_str(), nullptr, decimal_base);
    }
    else if (arg == "--threads" || arg == "-t")
    {
      args.threads = std::strtol(getNextArg().c_str(), nullptr, decimal_base);
    }
    else
    {
      std::cerr << "lemondb: warning: unknown argument " << arg << '\n';
    }
  }
}

void validateAndPrintThreads(std::int64_t threads)
{
  if (threads < 0)
  {
    std::cerr << "lemondb: error: threads num can not be negative value " << threads << '\n';
    std::exit(-1);
  }
  else if (threads == 0)
  {
    // @TODO Auto detect the thread num
    std::cerr << "lemondb: info: auto detect thread num" << '\n';
  }
  else
  {
    std::cerr << "lemondb: info: running in " << threads << " threads" << '\n';
  }
}

void setupParser(QueryParser& parser)
{
  parser.registerQueryBuilder(std::make_unique<DebugQueryBuilder>());
  parser.registerQueryBuilder(std::make_unique<ManageTableQueryBuilder>());
  parser.registerQueryBuilder(std::make_unique<ComplexQueryBuilder>());
}

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
} // namespace

int main(int argc, char* argv[])
{
  std::ios_base::sync_with_stdio(true);

  Args parsedArgs{};
  parseArgs(argc, argv, parsedArgs);

  std::ifstream fin;
  std::istream* input = &std::cin;
  if (!parsedArgs.listen.empty()) [[unlikely]]
  {
    // Construct a new ifstream and assign to fin to ensure all internal
    // members are properly initialized (avoids MSan use-of-uninitialized warnings).
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

  validateAndPrintThreads(parsedArgs.threads);

  QueryParser parser;
  setupParser(parser);

  // Main loop: ASYNC query submission with table-level parallelism
  // - Each table has its own execution thread
  // - Queries for different tables execute in parallel
  // - Results are buffered in OutputPool and printed at the end
  const Database& database = Database::getInstance();

  // Create OutputPool (not a global singleton - passed by reference)
  OutputPool output_pool;

  // Create QueryManager with reference to OutputPool
  QueryManager query_manager(output_pool);

  std::atomic<size_t> g_query_counter{0};

  while (input_stream && !database.isEnd()) [[likely]]
  {
    try
    {
      const std::string queryStr = extractQueryString(input_stream);

      // std::cerr << "[Main] Parsed query string (length=" << queryStr.length() << "): '"
      //           << queryStr << "'\n";

      Query::Ptr query = parser.parseQuery(queryStr);

      // Check if this is a QUIT query - need to check after trimming whitespace
      // Use a case-insensitive check for "QUIT"
      std::string trimmed = queryStr;
      // Trim leading whitespace
      size_t start = trimmed.find_first_not_of(" \t\n\r");
      if (start != std::string::npos)
      {
        trimmed = trimmed.substr(start);
      }

      if (query->isInstant() && trimmed.find("QUIT") == 0)
      {
        // std::cerr << "[Main] ========== QUIT DETECTED ==========\n";
        // std::cerr << "[Main] Breaking main loop, waiting for all queries to complete\n";
        // std::cerr << "[Main] ====================================\n";

        // Don't submit QUIT query - just break the loop
        break;
      }

      const size_t query_id = g_query_counter.fetch_add(1) + 1;

      // Get the target table for this query
      const std::string table_name = query->targetTableRef();

      // Handle COPYTABLE specially: add WaitQuery to the new table's queue
      if (trimmed.find("COPYTABLE") == 0)
      {
        // Cast to CopyTableQuery to get the wait semaphore
        auto* copy_query = dynamic_cast<CopyTableQuery*>(query.get());
        if (copy_query != nullptr)
        {
          auto wait_sem = copy_query->getWaitSemaphore();
          auto new_table_name = trimmed.substr(9); // Skip "COPYTABLE"
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

          // std::cerr << "[Main] COPYTABLE detected: source='" << table_name << "' target='"
          //           << new_table_name << "'\n";

          // Submit a WaitQuery to the new table's queue BEFORE the COPYTABLE query
          // NOTE: WaitQuery uses a special ID (0) since it's not a user query and shouldn't be
          // counted
          const size_t wait_query_id = 0; // Special ID for WaitQuery - not counted as user query
          auto wait_query = std::make_unique<WaitQuery>(table_name, wait_sem);
          query_manager.addQuery(wait_query_id, new_table_name, wait_query.release());

          // std::cerr << "[Main] WaitQuery submitted for new table '" << new_table_name
          //           << "' (query_id=" << wait_query_id << " - internal, not counted)\n";
        }
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
      std::cerr << "Error parsing query: " << exc.what() << '\n';
    }
  }

  // Tell manager how many queries we submitted
  const size_t total_queries = g_query_counter.load();
  // std::cerr << "[Main] Total queries submitted: " << total_queries << "\n";
  query_manager.setExpectedQueryCount(total_queries);

  // std::cerr << "[Main] All queries submitted, waiting for execution\n";

  // Wait for all queries to execute
  query_manager.waitForCompletion();

  // std::cerr << "[Main] ========== ALL QUERIES EXECUTED ==========\n";
  // std::cerr << "[Main] Dumping results from OutputPool\n";
  // std::cerr << "[Main] ===========================================\n";

  // Output all results in order (buffered via OutputPool)
  output_pool.outputAllResults();

  // std::cerr << "[Main] ========== RESULTS DUMPED ==========\n";
  // std::cerr << "[Main] Exiting program\n";
  // std::cerr << "[Main] =======================================\n";

  return 0;
}

#ifdef LEMONDB_WITH_MSAN
#undef LEMONDB_WITH_MSAN
#endif
