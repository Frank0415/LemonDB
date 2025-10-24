//
// Created by liu on 18-10-21.
//

#include <getopt.h>

#include <algorithm>
#include <atomic>
#include <fstream>
#include <future>
#include <iostream>
#include <string>
#include <vector>

#include "query/QueryBuilders.h"
#include "query/QueryParser.h"
#include "threading/Collector.h"
#include "threading/Threadpool.h"

struct {
  std::string listen;
  long threads = 0;
} parsedArgs;

// Global result collector and query counter
QueryResultCollector g_result_collector;
std::atomic<size_t> g_query_counter{0};

void parseArgs(int argc, char *argv[]) {
  const option longOpts[] = {{"listen", required_argument, nullptr, 'l'},
                             {"threads", required_argument, nullptr, 't'},
                             {nullptr, no_argument, nullptr, 0}};
  const char *shortOpts = "l:t:";
  int opt, longIndex;
  while ((opt = getopt_long(argc, argv, shortOpts, longOpts, &longIndex)) !=
         -1) {
    if (opt == 'l') {
      parsedArgs.listen = optarg;
    } else if (opt == 't') {
      parsedArgs.threads = std::strtol(optarg, nullptr, 10);
    } else {
      std::cerr << "lemondb: warning: unknown argument "
                << longOpts[longIndex].name << std::endl;
    }
  }
}

std::string extractQueryString(std::istream &is) {
  std::string buf;
  do {
    int ch = is.get();
    if (ch == ';')
      return buf;
    if (ch == EOF)
      throw std::ios_base::failure("End of input");
    buf.push_back((char)ch);
  } while (true);
}

int main(int argc, char *argv[]) {
  // Assume only C++ style I/O is used in lemondb
  // Do not use printf/fprintf in <cstdio> with this line
  std::ios_base::sync_with_stdio(false);

  parseArgs(argc, argv);

  std::ifstream fin;
  if (!parsedArgs.listen.empty()) {
    fin.open(parsedArgs.listen);
    if (!fin.is_open()) {
      std::cerr << "lemondb: error: " << parsedArgs.listen
                << ": no such file or directory" << std::endl;
      exit(-1);
    }
  }
  std::istream is(fin.rdbuf());

  ThreadPool::initialize(parsedArgs.threads > 0
                             ? static_cast<size_t>(parsedArgs.threads)
                             : std::thread::hardware_concurrency());
  ThreadPool &pool = ThreadPool::getInstance();

#ifdef NDEBUG
  // In production mode, listen argument must be defined
  if (parsedArgs.listen.empty()) {
    std::cerr << "lemondb: error: --listen argument not found, not allowed in "
                 "production mode"
              << std::endl;
    exit(-1);
  }
#else
  // In debug mode, use stdin as input if no listen file is found
  if (parsedArgs.listen.empty()) {
    std::cerr << "lemondb: warning: --listen argument not found, use stdin "
                 "instead in debug mode"
              << std::endl;
    is.rdbuf(std::cin.rdbuf());
  }
#endif

  if (parsedArgs.threads < 0) {
    std::cerr << "lemondb: error: threads num can not be negative value "
              << parsedArgs.threads << std::endl;
    exit(-1);
  } else if (parsedArgs.threads == 0) {
    // @TODO Auto detect the thread num
    std::cerr << "lemondb: info: auto detect thread num" << std::endl;
  } else {
    std::cerr << "lemondb: info: running in " << parsedArgs.threads
              << " threads" << std::endl;
  }

  QueryParser p;

  p.registerQueryBuilder(std::make_unique<QueryBuilder(Debug)>());
  p.registerQueryBuilder(std::make_unique<QueryBuilder(ManageTable)>());
  p.registerQueryBuilder(std::make_unique<QueryBuilder(Complex)>());

  // Main loop: async query submission
  std::vector<std::future<void>> pending_queries;

  while (is) {
    try {
      std::string queryStr = extractQueryString(is);
      Query::Ptr query = p.parseQuery(queryStr);

      size_t query_id = ++g_query_counter;

      // Async submit query (don't wait for completion)
      auto future = pool.submit([q = std::move(query), query_id]() mutable {
        executeQueryAsync(std::move(q), query_id, g_result_collector);
      });

      pending_queries.push_back(std::move(future));

      // Periodically clean completed futures to avoid memory accumulation
      if (pending_queries.size() > 100) {
        pending_queries.erase(
            std::remove_if(pending_queries.begin(), pending_queries.end(),
                           [](std::future<void> &f) {
                             return f.wait_for(std::chrono::seconds(0)) ==
                                    std::future_status::ready;
                           }),
            pending_queries.end());
      }

    } catch (const std::ios_base::failure &e) {
      break;
    } catch (const std::exception &e) {
      std::cerr << "Error parsing query: " << e.what() << std::endl;
    }
  }

  // Wait for all remaining queries to complete
  for (auto &future : pending_queries) {
    if (future.valid()) {
      future.wait();
    }
  }

  // Output all results in order
  g_result_collector.outputAllResults();

  return 0;
}
