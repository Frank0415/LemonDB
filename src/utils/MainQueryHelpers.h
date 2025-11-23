#ifndef PROJECT_MAIN_QUERY_HELPERS_H
#define PROJECT_MAIN_QUERY_HELPERS_H

#include <atomic>
#include <istream>
#include <optional>
#include <string>
#include <cstddef>

#include "../db/Database.h"
#include "../query/QueryParser.h"
#include "../query/management/CopyTableQuery.h"
#include "../query/utils/ListenQuery.h"
#include "../threading/QueryManager.h"
#include "../utils/MainUtils.h"

namespace MainQueryHelpers {
std::string extractQueryString(std::istream &input_stream);
std::string trimLeadingWhitespace(const std::string &str);
void handleListenQuery(ListenQuery *listen_query, QueryManager &query_manager,
                       std::atomic<size_t> &g_query_counter,
                       QueryParser &parser, Database &database,
                       bool &should_break);
void handleCopyTable(QueryManager &query_manager, const std::string &trimmed,
                     const std::string &table_name, CopyTableQuery *copy_query);
void processQueries(std::istream &input_stream, Database &database,
                    QueryParser &parser, QueryManager &query_manager,
                    std::atomic<size_t> &g_query_counter);
std::optional<size_t> setupListenMode(const Args &args, QueryParser &parser,
                                      Database &database,
                                      QueryManager &query_manager,
                                      std::atomic<size_t> &g_query_counter);
size_t
determineExpectedQueryCount(const std::optional<size_t> &listen_scheduled,
                            const std::atomic<size_t> &g_query_counter);
}  // namespace MainQueryHelpers

#endif  // PROJECT_MAIN_QUERY_HELPERS_H