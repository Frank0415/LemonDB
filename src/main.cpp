//
// Created by liu on 18-10-21.
//

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <fstream>
#include <getopt.h>
#include <iostream>
#include <memory>
#include <string>

#include "db/Database.h"
#include "query/QueryBuilders.h"
#include "query/QueryParser.h"
#include "threading/Collector.h"
#include "threading/Threadpool.h"
#include <unistd.h>

namespace
{
struct Args
{
  std::string listen;
  std::int64_t threads = 0;
};

void parseArgs(int argc, char** argv, Args& args)
{
  // Use std::array to avoid C-style array decay and allow safe indexing
  const std::array<option, 3> longOpts = {{{"listen", required_argument, nullptr, 'l'},
                                           {"threads", required_argument, nullptr, 't'},
                                           {nullptr, no_argument, nullptr, 0}}};

  const std::string shortOpts = "l:t:";
  int opt = 0;
  int longIndex = 0;
  while ((opt = getopt_long(argc, argv, shortOpts.c_str(), longOpts.data(), &longIndex)) != -1)
  {
    if (opt == 'l')
    {
      args.listen = optarg;
    }
    else if (opt == 't')
    {
      constexpr int decimal_base = 10;
      args.threads = std::strtol(optarg, nullptr, decimal_base);
    }
    else
    {
      // longIndex may be out of range for unknown options, guard access
      const char* optName = nullptr;
      if (longIndex >= 0 && static_cast<size_t>(longIndex) < longOpts.size())
      {
        const auto* const iter_begin = longOpts.begin();
        const auto* iter = iter_begin;
        using diff_t = std::iterator_traits<decltype(longOpts.begin())>::difference_type;
        std::advance(iter, static_cast<diff_t>(longIndex));
        optName = iter->name;
      }
      if (optName != nullptr)
      {
        std::cerr << "lemondb: warning: unknown argument " << optName << '\n';
      }
      else
      {
        std::cerr << "lemondb: warning: unknown argument" << '\n';
      }
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
  parser.registerQueryBuilder(std::make_unique<QueryBuilder(Debug)>());
  parser.registerQueryBuilder(std::make_unique<QueryBuilder(ManageTable)>());
  parser.registerQueryBuilder(std::make_unique<QueryBuilder(Complex)>());
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
    fin.open(parsedArgs.listen);
    if (!fin.is_open())
    {
      std::cerr << "lemondb: error: " << parsedArgs.listen << ": no such file or directory" << '\n';
      exit(-1);
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

  // Main loop: SEQUENTIAL query execution
  // Each query runs to completion before the next one starts
  // (but queries can use ThreadPool internally for parallelism)
  Database& database = Database::getInstance();

  QueryResultCollector g_result_collector;
  std::atomic<size_t> g_query_counter{0};

  while (input_stream && !database.isEnd())
  {
    try
    {
      std::string queryStr = extractQueryString(input_stream);

      Query::Ptr query = parser.parseQuery(queryStr);

      size_t query_id = g_query_counter.fetch_add(1) + 1;

      // ALL queries execute synchronously one after another
      // This ensures no data races between queries
      executeQueryAsync(std::move(query), query_id, g_result_collector);
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

  // Output all results in order
  g_result_collector.outputAllResults();

  return 0;
}

#ifdef LEMONDB_WITH_MSAN
#undef LEMONDB_WITH_MSAN
#endif
