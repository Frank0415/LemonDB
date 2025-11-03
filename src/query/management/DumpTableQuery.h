//
// Created by liu on 18-10-25.
//

#ifndef PROJECT_DUMPTABLEQUERY_H
#define PROJECT_DUMPTABLEQUERY_H

#include <string>
#include <utility>

#include "query/Query.h"

class DumpTableQuery : public Query
{
  static constexpr const char* qname = "DUMP";
  std::string fileName;

public:
  DumpTableQuery(std::string table, std::string filename)
      : Query(std::move(table)), fileName(std::move(filename))
  {
  }

  QueryResult::Ptr execute() override;

  std::string toString() override;

  [[nodiscard]] bool isWriter() const override
  {
    return false;
  } // DUMP only reads
  [[nodiscard]] bool isInstant() const override
  {
    return true;
  } // But must be serial
};

#endif // PROJECT_DUMPTABLEQUERY_H
