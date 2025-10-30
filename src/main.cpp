//
// Created by liu on 18-10-21.
//

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <fstream>
#include <future>
#include <getopt.h>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "db/Database.h"
#include "query/QueryBuilders.h"
#include "query/QueryParser.h"
#include "threading/Collector.h"
#include "threading/Threadpool.h"
#include <unistd.h>

#include "query/Query.h"

#ifdef __has_feature
#if __has_feature(memory_sanitizer)
#define LEMONDB_WITH_MSAN 1
#endif
#endif
#ifdef __SANITIZE_MEMORY__
#define LEMONDB_WITH_MSAN 1
#endif

namespace
{
struct Args
{
  std::string listen;
  std::int64_t threads = 0;
};

void parseArgs(int argc, char* argv[], Args& args)
{
  const option longOpts[] = {{"listen", required_argument, nullptr, 'l'},
                             {"threads", required_argument, nullptr, 't'},
                             {nullptr, no_argument, nullptr, 0}};
  const char* shortOpts = "l:t:";
  int opt = 0;
  int longIndex = 0;
  while ((opt = getopt_long(argc, argv, shortOpts, longOpts, &longIndex)) != -1)
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
      std::cerr << "lemondb: warning: unknown argument " << longOpts[longIndex].name << '\n';
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
  ThreadPool& pool = ThreadPool::getInstance();

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

  // Main loop: async query submission
  std::vector<std::future<void>> pending_queries;
  Database& database = Database::getInstance();

  QueryResultCollector g_result_collector;
  std::atomic<size_t> g_query_counter{0};

  auto prune_completed = [&pending_queries]()
  {
    pending_queries.erase(std::remove_if(pending_queries.begin(), pending_queries.end(),
                                         [](std::future<void>& future)
                                         {
                                           if (!future.valid())
                                           {
                                             return true;
                                           }
                                           return future.wait_for(std::chrono::seconds(0)) ==
                                                  std::future_status::ready;
                                         }),
                          pending_queries.end());
  };

  while (input_stream && !database.isEnd())
  {
    try
    {
      std::string queryStr = extractQueryString(input_stream);

      Query::Ptr query = parser.parseQuery(queryStr);

      size_t query_id = g_query_counter.fetch_add(1) + 1;

      // Check if this is an instant query (LOAD, QUIT, DROP, DUMP)
      if (query->isInstant())
      {
        // Wait for all pending queries to complete before executing instant
        // query
        for (auto& f : pending_queries)
        {
          f.wait();
        }
        pending_queries.clear();

        // Execute instant query synchronously (in main thread)
        executeQueryAsync(std::move(query), query_id, g_result_collector);
      }
      else
      {
        prune_completed();

        const size_t worker_count = std::max<size_t>(1, pool.getThreadCount());
        const size_t max_parallel = worker_count > 1 ? worker_count - 1 : 1;

        while (pending_queries.size() >= max_parallel)
        {
          if (!pending_queries.empty())
          {
            auto& front_future = pending_queries.front();
            if (front_future.valid())
            {
              front_future.wait();
            }
            pending_queries.erase(pending_queries.begin());
          }
          prune_completed();
        }

        // Async submit query (don't wait for completion)
        auto future =
            pool.submit([q = std::move(query), query_id, &g_result_collector]() mutable
                        { executeQueryAsync(std::move(q), query_id, g_result_collector); });

        pending_queries.push_back(std::move(future));
      }
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

  // Wait for all remaining queries to complete
  for (size_t i = 0; i < pending_queries.size(); ++i)
  {
    if (pending_queries[i].valid())
    {
      pending_queries[i].wait();
    }
  }

  // Output all results in order
  g_result_collector.outputAllResults();

  return 0;
}

#ifdef LEMONDB_WITH_MSAN
#undef LEMONDB_WITH_MSAN
#endif
