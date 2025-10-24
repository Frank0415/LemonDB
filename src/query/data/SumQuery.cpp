#include "query/data/SumQuery.h"
#include "db/Database.h"
#include "db/TableLockManager.h"
#include "query/QueryResult.h"
#include "threading/Threadpool.h"

QueryResult::Ptr SumQuery::execute() {
  using namespace std;

  try {
    Database &db = Database::getInstance();
    auto &table = db[this->targetTable];
    if (this->operands.empty()) {
      return make_unique<ErrorMsgResult>("SUM", this->targetTable,
                                         "Invalid number of fields");
    }
    for (const auto &f : this->operands) {
      if (f == "KEY") {
        return make_unique<ErrorMsgResult>("SUM", this->targetTable,
                                           "KEY cannot be summed.");
      }
    }
    auto result = initCondition(table);
    if (!result.second) {
      throw IllFormedQueryCondition("Error conditions in WHERE clause.");
    }
    vector<Table::FieldIndex> fids;
    fids.reserve(this->operands.size());
    for (const auto &f : this->operands) {
      fids.emplace_back(table.getFieldIndex(f));
    }

    // NEW: LOCK IMPL
    auto &lockmgr = TableLockManager::getInstance();
    auto lock = lockmgr.acquireRead(this->targetTable);

    // NEW: READ LOCK PROTECTED MODE
    static ThreadPool &pool = ThreadPool::getInstance();
    // const size_t chunk_size = getChunkSize();
    const size_t chunk_size = 2000;
    const size_t num_fields = fids.size();

    std::vector<std::future<std::vector<int>>> futures;

    futures.push_back(pool.submit());

    vector<int> sums(fids.size(), 0);
    bool handled =
        this->testKeyCondition(table, [&](bool ok, Table::Object::Ptr &&obj) {
          if (!ok)
            return;
          if (obj) {
            for (size_t i = 0; i < fids.size(); ++i) {
              sums[i] += (*obj)[fids[i]];
            }
          }
        });
    if (!handled) {
      for (auto it = table.begin(); it != table.end(); ++it) {
        if (this->evalCondition(*it)) {
          for (size_t i = 0; i < fids.size(); ++i) {
            sums[i] += (*it)[fids[i]];
          }
        }
      }
    }
    return make_unique<SuccessMsgResult>(sums);

  } catch (const TableNameNotFound &) {
    return make_unique<ErrorMsgResult>("SUM", this->targetTable,
                                       "No such table.");
  } catch (const TableFieldNotFound &) {
    return make_unique<ErrorMsgResult>("SUM", this->targetTable,
                                       "No such field.");
  } catch (const IllFormedQueryCondition &e) {
    return make_unique<ErrorMsgResult>("SUM", this->targetTable, e.what());
  } catch (const std::exception &e) {
    return make_unique<ErrorMsgResult>("SUM", this->targetTable,
                                       "Unknown error '?'"_f % e.what());
  }
}

std::string SumQuery::toString() {
  return "QUERY = SUM \"" + this->targetTable + "\"";
}