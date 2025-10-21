#include <iostream>
#include <sstream>
#include <vector>
#include <string>
#include <functional>
#include <memory>

#include "db/Database.h"
#include "db/Table.h"
#include "query/Query.h"
#include "query/QueryResult.h"
#include "query/data/SwapQuery.h"

// Minimal test harness
struct Suite {
  int pass = 0, fail = 0;
  void ok(const std::string& name, bool cond, const std::string& msg = "") {
    if (cond) { ++pass; std::cerr << "[PASS] " << name << "\n"; }
    else { ++fail; std::cerr << "[FAIL] " << name << (msg.empty() ? "" : "  " + msg) << "\n"; }
  }
  int summary() const {
    std::cerr << "\n=== SWAP TESTS: " << pass << " passed, " << fail << " failed ===\n";
    return fail == 0 ? 0 : 1;
  }
};

// Build Student table
static void seedStudent() {
  auto tbl = std::make_unique<Table>("Student",
      std::vector<std::string>{"studentID","class","totalCredit"});
  // Keys, followed by values (int32 safe)
  tbl->insertByIndex("Bill_Gates", {400812312, 2014, 112});
  tbl->insertByIndex("Steve_Jobs", {400851751, 2014, 115});
  tbl->insertByIndex("Jack_Ma",    {400882382, 2015, 123});
  Database::getInstance().registerTable(std::move(tbl));
}

// Run a SwapQuery directly
static QueryResult::Ptr run_swap(
    const std::string& table,
    std::vector<std::string> operands,
    std::vector<QueryCondition> conds) {
  SwapQuery q(table, std::move(operands), std::move(conds));
  return q.execute();
}

static QueryCondition C(const std::string& f, const std::string& op, const std::string& v) {
  QueryCondition c; c.field=f; c.op=op; c.value=v; return c;
}

static int geti(const Table& t, const std::string& key, const std::string& field) {
  auto obj = const_cast<Table&>(t)[key];
  if (!obj) throw std::runtime_error("row not found: "+key);
  return (*obj)[ t.getFieldIndex(field) ];
}

int main() {
  Suite T;
  auto& db = Database::getInstance();
  seedStudent();
  auto& stu = db["Student"];

  // Case 1: Swap (class, studentID) where class < 2015
  {
    auto res = run_swap("Student", {"class","studentID"},
                        { C("class","<","2015") });
    std::ostringstream oss; if (res->display()) oss << *res;
    T.ok("affected==2", oss.str().find("Affected 2 rows.") != std::string::npos);
    T.ok("Bill_Gates studentID=2014", geti(stu,"Bill_Gates","studentID") == 2014);
    T.ok("Bill_Gates class=400812312", geti(stu,"Bill_Gates","class") == 400812312);
    T.ok("Steve_Jobs studentID=2014", geti(stu,"Steve_Jobs","studentID") == 2014);
    T.ok("Steve_Jobs class=400851751", geti(stu,"Steve_Jobs","class") == 400851751);
    T.ok("Jack_Ma unchanged id", geti(stu,"Jack_Ma","studentID") == 400882382);
    T.ok("Jack_Ma unchanged class", geti(stu,"Jack_Ma","class") == 2015);
  }

  // Case 2: WHERE no match
  {
    auto res = run_swap("Student", {"class","studentID"},
                        { C("class","<","1900") });
    std::ostringstream oss; if (res->display()) oss << *res;
    T.ok("affected==0", oss.str().find("Affected 0 rows.") != std::string::npos);
  }

  // Case 3: Swap identical fields
  {
    auto res = run_swap("Student", {"class","class"}, {});
    std::ostringstream oss; if (res->display()) oss << *res;
    T.ok("same-field affected==0", oss.str().find("Affected 0 rows.") != std::string::npos);
  }

  // Case 4: Full table swap (swap back to restore state)
  {
    auto res = run_swap("Student", {"class","studentID"}, {});
    std::ostringstream oss; if (res->display()) oss << *res;
    T.ok("fulltable affected==3", oss.str().find("Affected 3 rows.") != std::string::npos);
    T.ok("Bill_Gates id restored", geti(stu,"Bill_Gates","studentID") == 400812312);
    T.ok("Bill_Gates class restored", geti(stu,"Bill_Gates","class") == 2014);
    T.ok("Jack_Ma swapped id", geti(stu,"Jack_Ma","studentID") == 2015);
    T.ok("Jack_Ma swapped class", geti(stu,"Jack_Ma","class") == 400882382);
  }

  return T.summary();
}