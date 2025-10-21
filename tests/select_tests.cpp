#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <functional>

#include "db/Database.h"
#include "db/Table.h"
#include "query/Query.h"
#include "query/data/SelectQuery.h"

struct TestSuite {
  int passed = 0, failed = 0;

  void checkEqual(const std::string& name,
                  const std::string& got,
                  const std::string& expect) {
    if (got == expect) {
      ++passed;
      std::cerr << "[PASS] " << name << "\n";
    } else {
      ++failed;
      std::cerr << "[FAIL] " << name << "\n";
      std::cerr << "----- got -----\n" << got
                << "----- expect --\n" << expect
                << "----------------\n";
    }
  }

  void checkError(const std::string& name,
                  const std::string& errContains,
                  const std::function<void()>& run) {
    try {
      run();
      ++failed;
      std::cerr << "[FAIL] " << name << " (no exception thrown)\n";
    } catch (const std::exception& e) {
      std::string what = e.what();
      if (what.find(errContains) != std::string::npos) {
        ++passed;
        std::cerr << "[PASS] " << name << "\n";
      } else {
        ++failed;
        std::cerr << "[FAIL] " << name << " (unexpected error)\n";
        std::cerr << "what(): " << what << "\n";
      }
    }
  }

  int summary() const {
    std::cerr << "\n=== SELECT TESTS: " << passed << " passed, "
              << failed << " failed ===\n";
    return failed == 0 ? 0 : 1;
  }
};
static std::string run_select(const std::string& tableName,
                              std::vector<std::string> operands,
                              std::vector<QueryCondition> conds) {
  SelectQuery q(tableName, std::move(operands), std::move(conds));
  QueryResult::Ptr res = q.execute();

  std::ostringstream oss;
  if (res->success() && res->display()) {
    oss << *res; 
  } else if (!res->success()) {
    oss << *res;
  } else {
    oss << "[no-display-success]\n";
  }
  return oss.str();
}
static void seedEmployees(Database& db) {
  auto tbl = std::make_unique<Table>("EMP", std::vector<std::string>{"age", "salary", "dept"});
  tbl->insertByIndex("e001", {25, 5000, 10});
  tbl->insertByIndex("e010", {31, 8800, 20});
  tbl->insertByIndex("e003", {25, 7000, 30});
  tbl->insertByIndex("e002", {40, 12000, 20});
  tbl->insertByIndex("e005", {28, 6500, 10});
  db.registerTable(std::move(tbl));
}

static QueryCondition C(const std::string& field,
                        const std::string& op,
                        const std::string& value) {
  QueryCondition qc;
  qc.field = field;
  qc.op = op;
  qc.value = value;
  return qc;
}

int main() {
  TestSuite T;
  Database& db = Database::getInstance();
  seedEmployees(db);

  {
    std::string out = run_select("EMP", {"KEY", "age", "salary"}, {});
    std::string expect;
    expect += "( e001 25 5000 )\n";
    expect += "( e002 40 12000 )\n";
    expect += "( e003 25 7000 )\n";
    expect += "( e005 28 6500 )\n";
    expect += "( e010 31 8800 )\n";
    T.checkEqual("SELECT all (KEY age salary)", out, expect);
  }
  {
    std::string out = run_select("EMP", {"KEY", "salary"}, { C("KEY", "=", "e002") });
    std::string expect = "( e002 12000 )\n";
    T.checkEqual("SELECT where KEY= (fast path)", out, expect);
  }
  {
    std::string out = run_select(
      "EMP",
      {"KEY", "dept", "salary"},
      { C("age", ">=", "28"), C("salary", "<", "9000") }
    );
    std::string expect;
    expect += "( e005 10 6500 )\n";
    expect += "( e010 20 8800 )\n";
    T.checkEqual("SELECT where (age>=28 && salary<9000)", out, expect);
  }

  {
    std::string out = run_select("EMP", {"KEY", "salary"}, { C("dept", "=", "99") });
    std::string expect = ""; 
    T.checkEqual("SELECT no match prints nothing", out, expect);
  }

  {
    std::string out = run_select("EMP", {"KEY", "wrongField"}, {});
    if (out.find("QUERY FAILED") != std::string::npos || out.find("No such field") != std::string::npos) {
      T.passed++; std::cerr << "[PASS] SELECT wrong field -> error\n";
    } else {
      T.failed++; std::cerr << "[FAIL] SELECT wrong field -> error\nGot:\n" << out;
    }
  }
  {
    std::string out = run_select("NOTABLE", {"KEY", "salary"}, {});
    if (out.find("QUERY FAILED") != std::string::npos || out.find("No such table") != std::string::npos) {
      T.passed++; std::cerr << "[PASS] SELECT no such table -> error\n";
    } else {
      T.failed++; std::cerr << "[FAIL] SELECT no such table -> error\nGot:\n" << out;
    }
  }

  return T.summary();
}