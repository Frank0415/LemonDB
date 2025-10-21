#include <sstream>
#include <algorithm>

#include "SelectQuery.h"
#include "../../db/Database.h"
#include "../QueryResult.h"

constexpr const char* SelectQuery::qname;

QueryResult::Ptr SelectQuery::execute() {
  using namespace std;
  try {
    auto& db = Database::getInstance();
    auto& table = db[this->targetTable];
    if (this->operands.empty()) {
      return make_unique<ErrorMsgResult>(
        qname, this->targetTable, "Invalid operands.");
    }
    vector<string> fieldsOrder;
    fieldsOrder.reserve(this->operands.size() + 1);
    fieldsOrder.emplace_back("KEY");
    for (const auto& f : this->operands) if (f != "KEY") fieldsOrder.emplace_back(f);
    vector<Table::FieldIndex> fieldIds;
    fieldIds.reserve(fieldsOrder.size() - 1);
    for (size_t i = 1; i < fieldsOrder.size(); ++i) {
      fieldIds.emplace_back(table.getFieldIndex(fieldsOrder[i]));
    }

    ostringstream buffer;
    bool handled = this->testKeyCondition(
      table,
      [&](bool ok, Table::Object::Ptr&& obj){
        if (!ok) return; 
        if (obj) {
          buffer << "( " << obj->key();
          for (size_t j = 0; j < fieldIds.size(); ++j)
            buffer << " " << (*obj)[fieldIds[j]];
          buffer << " )\n";
        }
      }
    );
    if (handled) {
      return make_unique<TextRowsResult>(buffer.str());
    }

    vector<Table::Iterator> rows;
    for (auto it = table.begin(); it != table.end(); ++it){
      if (this->evalCondition(*it)) rows.push_back(it);
    }
    sort(rows.begin(), rows.end(),
         [](const Table::Iterator& a, const Table::Iterator& b){
           return (*a).key() < (*b).key();
         });

    for (const auto& it : rows){
      buffer << "( " << (*it).key();
      for (size_t j = 0; j < fieldIds.size(); ++j)
        buffer << " " << (*it)[ fieldIds[j] ];
      buffer << " )\n";
    }
    return make_unique<TextRowsResult>(buffer.str());
  } catch (const TableNameNotFound&){
    return make_unique<ErrorMsgResult>(qname, this->targetTable, "No such table.");
  } catch (const IllFormedQueryCondition& e){
    return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
  } catch (const std::exception& e){
    return make_unique<ErrorMsgResult>(qname, this->targetTable,
                                       "Unknown error '?'."_f % e.what());
  }
}