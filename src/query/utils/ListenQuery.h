#ifndef LEMONDB_SRC_QUERY_DATA_LISTENQUERY_H
#define LEMONDB_SRC_QUERY_DATA_LISTENQUERY_H

#include <cstddef>
#include <string>
#include <utility>

#include "db/QueryBase.h"
#include <atomic>

class QueryManager;
class QueryParser;
class Database;

class ListenQuery : public Query
{
  static constexpr const char* qname = "LISTEN";
  std::string fileName;

  QueryManager* query_manager = nullptr;
  QueryParser* query_parser = nullptr;
  Database* database = nullptr;
  std::atomic<size_t>* query_counter = nullptr; // ADD THIS

  size_t scheduled_query_count = 0;
  bool quit_encountered = false;

  [[nodiscard]] bool shouldSkipStatement(const std::string& trimmed) const;
  bool processStatement(const std::string& trimmed);

public:
  explicit ListenQuery(std::string filename)
      : Query("__listen_table"), fileName(std::move(filename))
  {
  }

  void setDependencies(QueryManager* manager,
                       QueryParser* parser,
                       Database* database_ptr,
                       std::atomic<size_t>* counter); // ADD PARAMETER

  [[nodiscard]] size_t getScheduledQueryCount() const
  {
    return scheduled_query_count;
  }

  [[nodiscard]] bool hasEncounteredQuit() const
  {
    return quit_encountered;
  }

  [[nodiscard]] const std::string& getFileName() const
  {
    return fileName;
  }

  QueryResult::Ptr execute() override;

  std::string toString() override;

  [[nodiscard]] bool isInstant() const override
  {
    return true;
  }
};

#endif // LEMONDB_SRC_QUERY_DATA_LISTENQUERY_H
