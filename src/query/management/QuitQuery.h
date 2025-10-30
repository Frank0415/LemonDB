//
// Created by liu on 18-10-25.
//

#ifndef PROJECT_QUITQUERY_H
#define PROJECT_QUITQUERY_H

#include <string>

#include "../Query.h"
#include "../QueryResult.h"

class QuitQuery : public Query
{
  static constexpr const char* qname = "QUIT";

public:
  QuitQuery() = default;

  QueryResult::Ptr execute() override;

  std::string toString() override;

  // QUIT is not a writer (doesn't modify data)
  [[nodiscard]] bool isWriter() const override { return false; }

  // QUIT must execute immediately and serially (not parallel)
  [[nodiscard]] bool isInstant() const override { return true; }
};

#endif // PROJECT_QUITQUERY_H
