#include <algorithm>
#include <sstream>
#include "../../db/Database.h"
#include "../QueryResult.h"
#include "AddQuery.h"

constexpr const char *AddQuery::qname;

QueryResult::Ptr AddQuery::execute() { using namespace std; }

std::string AddQuery::toString() {
  return "QUERY = Duplicate TABLE \"" + this->targetTable + "\"";
}