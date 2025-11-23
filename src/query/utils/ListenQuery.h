#ifndef LEMONDB_SRC_QUERY_DATA_LISTENQUERY_H
#define LEMONDB_SRC_QUERY_DATA_LISTENQUERY_H

#include <atomic>
#include <cstddef>
#include <deque>
#include <memory>
#include <string>
#include <utility>

#include "../../db/QueryBase.h"
#include "../QueryResult.h"

class QueryManager;
class QueryParser;
class Database;

class ListenQuery : public Query {
  static constexpr const char *qname = "LISTEN";
  std::string fileName;

  QueryManager *query_manager = nullptr;
  QueryParser *query_parser = nullptr;
  Database *database = nullptr;
  std::atomic<size_t> *query_counter = nullptr;
  std::deque<std::unique_ptr<ListenQuery>> *pending_listens = nullptr;

  size_t scheduled_query_count = 0;
  bool quit_encountered = false;
  size_t id = 0;

  static bool shouldSkipStatement(const std::string &trimmed);
  bool processStatement(const std::string &trimmed,
                        std::string *nested_file_out = nullptr);

public:
  explicit ListenQuery(std::string filename)
      : Query("__listen_table"), fileName(std::move(filename)) {}

  void setId(size_t query_id) { id = query_id; }
  [[nodiscard]] size_t getId() const { return id; }

  void setDependencies(
      QueryManager *manager, QueryParser *parser, Database *database_ptr,
      std::atomic<size_t> *counter,
      std::deque<std::unique_ptr<ListenQuery>> *pending_queue = nullptr);

  [[nodiscard]] size_t getScheduledQueryCount() const {
    return scheduled_query_count;
  }

  [[nodiscard]] bool hasEncounteredQuit() const { return quit_encountered; }

  [[nodiscard]] const std::string &getFileName() const { return fileName; }

  QueryResult::Ptr execute() override;

  std::string toString() override;

  [[nodiscard]] bool isInstant() const override { return true; }
};

#endif  // LEMONDB_SRC_QUERY_DATA_LISTENQUERY_H
