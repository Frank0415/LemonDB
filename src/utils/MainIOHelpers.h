#ifndef PROJECT_MAIN_IO_HELPERS_H
#define PROJECT_MAIN_IO_HELPERS_H

#include <fstream>
#include <istream>
#include <string>

#include "../threading/OutputPool.h"
#include "../threading/QueryManager.h"
#include "../utils/MainUtils.h"
#include "../utils/OutputConfig.h"

namespace MainIOHelpers {
std::istream *initializeInputStream(const Args &parsedArgs,
                                    std::ifstream &fin);
void validateProductionMode(const Args &parsedArgs);
void flushOutputLoop(OutputPool &output_pool, const QueryManager &query_manager,
                     const OutputConfig &output_config);
}  // namespace MainIOHelpers

#endif  // PROJECT_MAIN_IO_HELPERS_H