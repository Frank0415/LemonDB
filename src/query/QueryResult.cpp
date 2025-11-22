//
// Created by liu on 18-10-26.
//

#include "QueryResult.h"

#include <ostream>

std::ostream &operator<<(std::ostream &out, const QueryResult &table) {
  return table.output(out);
}
