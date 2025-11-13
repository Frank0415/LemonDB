#ifndef PROJECT_MAIN_UTILS_H
#define PROJECT_MAIN_UTILS_H

#include <string>

#include "query/QueryParser.h"

struct Args {
  std::string listen;
  std::int64_t threads = 0;
};

namespace MainUtils {
void parseArgs(int argc, char **argv, Args &args);

void setupParser(QueryParser &parser);
}  // namespace MainUtils

#endif  // PROJECT_MAIN_UTILS_H