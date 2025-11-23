#ifndef PROJECT_MAIN_UTILS_H
#define PROJECT_MAIN_UTILS_H

#include <cstdint>
#include <string>

#include "../query/QueryParser.h"

/**
 * Command line argument bundle for main program.
 *
 * Fields:
 *  - listen: Path or identifier for a LISTEN source (empty if not specified).
 *  - threads: Requested thread count override (0 means use default / auto).
 */
struct Args {
  /** Path or identifier provided to LISTEN related option (may be empty). */
  std::string listen;
  /** Explicit thread count requested by user; 0 selects automatic hardware
   * concurrency. */
  std::int64_t threads = 0;
};

namespace MainUtils {
/**
 * Parse command line arguments populating an Args structure.
 * Recognized options typically include LISTEN source and thread overrides.
 * @param argc Argument count from main.
 * @param argv Argument vector from main.
 * @param args Output struct populated with parsed values (existing values
 * overwritten).
 */
void parseArgs(int argc, char **argv, Args &args);

/**
 * Configure a QueryParser instance with standard builders / handlers
 * required for processing user queries.
 * @param parser QueryParser instance to initialize.
 */
void setupParser(QueryParser &parser);

/**
 * Check if the workload is small enough to run in single-threaded mode.
 * @param filepath Path to the listen file.
 * @return True if the workload is small, false otherwise.
 */
bool checkSmallWorkload(const std::string &filepath);
}  // namespace MainUtils

#endif  // PROJECT_MAIN_UTILS_H
