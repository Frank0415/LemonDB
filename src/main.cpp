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
    if (character == ';')
    {
      return buf;
    }
    if (character == EOF)
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
  if (!parsedArgs.listen.empty())
  {
    // Construct a new ifstream and assign to fin to ensure all internal
    // members are properly initialized (avoids MSan use-of-uninitialized warnings).
    fin = std::ifstream(parsedArgs.listen);
    if (!fin.is_open())
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

  while (input_stream && !database.isEnd())
  {
    try
    {
      const std::string queryStr = extractQueryString(input_stream);


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
          auto new_table_name = trimmed.substr(9);  // Skip "COPYTABLE"
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

        }
      }

      // Submit query to manager (async - doesn't block)
      // Manager will create per-table thread if needed
      query_manager.addQuery(query_id, table_name, query.release());
    }
    catch (const std::ios_base::failure& e)
    {
      break;
    }
    catch (const std::exception& e)
    {
      std::cerr << "Error parsing query: " << e.what() << '\n';
    }
  }



  return 0;
}

#ifdef LEMONDB_WITH_MSAN
#undef LEMONDB_WITH_MSAN
#endif
