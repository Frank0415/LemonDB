//
// Created by liu on 18-10-26.
//

#include "QueryResult.h"

#include <ostream>

std::ostream &operator<<(std::ostream &out, const QueryResult &table) {
  return table.output(out);
}

std::string QueryResult::buildMessage(std::string &&msg) {
  return std::move(msg);
}

std::string QueryResult::buildMessage(const std::vector<int> &results) {
  std::stringstream stream;
  stream << "ANSWER = ( ";
  for (auto result : results) {
    stream << result << " ";
  }
  stream << ")";
  return stream.str();
}