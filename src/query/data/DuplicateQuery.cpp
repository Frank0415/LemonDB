#include <algorithm>
#include <sstream>

#include "../../db/Database.h"
#include "../QueryResult.h"
#include "DuplicateQuery.h"

constexpr const char *DuplicateQuery::qname;

QueryResult::Ptr DuplicateQuery::execute() {
  using namespace std;
  try {
    if (!this->operands.empty()) {
      return make_unique<ErrorMsgResult>(
          qname, this->targetTable,
          "Invalid number of operands (? operands)."_f % operands.size());
    }
    auto &db = Database::getInstance();
    auto &table = db[this->targetTable];
    auto result = initCondition(table);
    if (!result.second) {
      throw IllFormedQueryCondition("Error conditions in WHERE clause.");
    }

    std::vector<const Table::KeyType *> keysToDuplicate;

    for (auto it = table.begin(); it != table.end(); ++it) {
      if (this->evalCondition(*it)) {
        auto originalKey = it->key();

        if (!table.evalDuplicateCopy(originalKey)) {
          continue;
        }

        keysToDuplicate.push_back(&originalKey);
      }
    }

    for (const auto *key : keysToDuplicate) {
      auto originalObject = table[*key];
    }

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

std::string DuplicateQuery::toString() {
  return "QUERY = DUPLICATE " + this->targetTable + "\"";
}