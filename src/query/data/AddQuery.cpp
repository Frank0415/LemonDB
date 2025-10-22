#include "AddQuery.h"
#include "../../db/Database.h"
#include "../QueryResult.h"
#include <algorithm>
#include <sstream>

constexpr const char *AddQuery::qname;

QueryResult::Ptr AddQuery::execute() {
  using namespace std;
  try {
    if (this->operands.size() < 2) {
      return make_unique<ErrorMsgResult>(
          qname, this->targetTable,
          "Invalid number of operands (? operands)."_f % operands.size());
    }
    auto &db = Database::getInstance();
    auto &table = db[this->targetTable];

  } catch (const NotFoundKey &e) {
    return make_unique<ErrorMsgResult>(qname, this->targetTable,
                                       "Key not found."s);
  } catch (const TableNameNotFound &e) {
    return make_unique<ErrorMsgResult>(qname, this->targetTable,
                                       "No such table."s);
  } catch (const IllFormedQueryCondition &e) {
    return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
  } catch (const invalid_argument &e) {
    // Cannot convert operand to string
    return make_unique<ErrorMsgResult>(qname, this->targetTable,
                                       "Unknown error '?'"_f % e.what());
  } catch (const exception &e) {
    return make_unique<ErrorMsgResult>(qname, this->targetTable,
                                       "Unkonwn error '?'."_f % e.what());
  }
}

std::string AddQuery::toString() {
  return "QUERY = Duplicate TABLE \"" + this->targetTable + "\"";
}