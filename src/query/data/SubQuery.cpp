#include "SubQuery.h"
#include "../../db/Database.h"
#include "../QueryResult.h"
#include <algorithm>
#include <sstream>

constexpr const char *SubQuery::qname;

QueryResult::Ptr SubQuery::execute() {}

std::string SubQuery::toString() {
  return "QUERY = Duplicate TABLE \"" + this->targetTable + "\"";
}