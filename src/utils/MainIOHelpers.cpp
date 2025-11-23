#include "MainIOHelpers.h"

#include <iostream>
#include <thread>

namespace MainIOHelpers {
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

void flushOutputLoop(OutputPool &output_pool, const QueryManager &query_manager,
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
}  // namespace MainIOHelpers