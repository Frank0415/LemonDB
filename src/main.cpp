#include <atomic>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <exception>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <utility>

#include "db/Database.h"
#include "db/QueryBase.h"
#include "query/QueryParser.h"
#include "query/management/CopyTableQuery.h"
#include "query/management/WaitQuery.h"
#include "query/utils/ListenQuery.h"
#include "threading/OutputPool.h"
#include "threading/QueryManager.h"
#include "threading/Threadpool.h"
#include "utils/MainIOHelpers.h"
#include "utils/MainQueryHelpers.h"
#include "utils/MainUtils.h"
#include "utils/OutputConfig.h"

int main(int argc, char *argv[]) {
  try {
    std::ios_base::sync_with_stdio(true);

    Args parsedArgs{};
    MainUtils::parseArgs(argc, argv, parsedArgs);

    std::ifstream fin;
    std::istream *input = MainIOHelpers::initializeInputStream(parsedArgs, fin);

    ThreadPool::initialize(parsedArgs.threads > 0
                               ? static_cast<size_t>(parsedArgs.threads)
                               : std::thread::hardware_concurrency());

    MainIOHelpers::validateProductionMode(parsedArgs);

    QueryParser parser;
    MainUtils::setupParser(parser);

    // Main loop: ASYNC query submission with table-level parallelism
    Database &database = Database::getInstance();

    // Create OutputPool (not a global singleton - passed by reference)
    OutputPool output_pool;

    // Create QueryManager with reference to OutputPool
    QueryManager query_manager(output_pool);

    std::atomic<size_t> g_query_counter{0};

    const auto listen_scheduled = MainQueryHelpers::setupListenMode(
        parsedArgs, parser, database, query_manager, g_query_counter);

    const OutputConfig output_config{};

    if (!listen_scheduled.has_value()) {
      query_manager.setExpectedQueryCount(std::numeric_limits<size_t>::max());
      std::thread flush_thread(MainIOHelpers::flushOutputLoop, std::ref(output_pool),
                               std::ref(query_manager), output_config);
      MainQueryHelpers::processQueries(*input, database, parser, query_manager, g_query_counter);
      query_manager.setExpectedQueryCount(g_query_counter.load());
      flush_thread.join();
    } else {
      const size_t total_queries =
          MainQueryHelpers::determineExpectedQueryCount(listen_scheduled, g_query_counter);
      query_manager.setExpectedQueryCount(total_queries);
      MainIOHelpers::flushOutputLoop(output_pool, query_manager, output_config);
    }

    query_manager.waitForCompletion();
    output_pool.outputAllResults();
  } catch (...) {
    // TODO: NOTHING SHOULD BE HANDLED
    return -1;
  }

  return 0;
}

#ifdef LEMONDB_WITH_MSAN
#undef LEMONDB_WITH_MSAN
#endif
