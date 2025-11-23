#include "MainUtils.h"

#include <cstdlib>
#include <cstring>
#include <memory>
#include <span>
#include <string>

#include "query/QueryBuilders.h"
#include "query/QueryParser.h"

namespace MainUtils {
void parseArgs(int argc, char **argv, Args &args) {
  // Manual argument parser supporting both long and short forms
  // --listen=<file> or --listen <file> or -l <file>
  // --threads=<num> or --threads <num> or -t <num>

  constexpr size_t listen_prefix_len = 9;    // Length of "--listen="
  constexpr size_t threads_prefix_len = 10;  // Length of "--threads="
  constexpr int decimal_base = 10;

  for (int i = 1; i < argc; ++i) {
    const std::string arg(std::span(
        argv, static_cast<std::size_t>(argc))[static_cast<std::size_t>(i)]);

    // Helper lambda to get next argument value
    auto getNextArg = [&]() -> std::string {
      if (i + 1 < argc) {
        ++i;
        return {std::span(
            argv, static_cast<std::size_t>(argc))[static_cast<std::size_t>(i)]};
      }
      (void)arg;
      std::exit(-1);
    };

    // Handle --listen=<value> or --listen <value>
    if (arg.starts_with("--listen=") || arg == "--listen" || arg == "-l") {
      if (arg.starts_with("--listen=")) {
        args.listen = arg.substr(listen_prefix_len);
      } else {
        args.listen = getNextArg();
      }
      continue;
    }

    // Handle --threads=<value> or --threads <value>
    if (arg.starts_with("--threads=") || arg == "--threads" || arg == "-t") {
      std::string value;
      if (arg.starts_with("--threads=")) {
        value = arg.substr(threads_prefix_len);
      } else {
        value = getNextArg();
      }
      args.threads = std::strtol(value.c_str(), nullptr, decimal_base);
      continue;
    }

    (void)arg;
  }
}

void setupParser(QueryParser &parser) {
  parser.registerQueryBuilder(std::make_unique<DebugQueryBuilder>());
  parser.registerQueryBuilder(std::make_unique<ManageTableQueryBuilder>());
  parser.registerQueryBuilder(std::make_unique<ComplexQueryBuilder>());
}
}  // namespace MainUtils