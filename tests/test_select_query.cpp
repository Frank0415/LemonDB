#include <gtest/gtest.h>
#include <sstream>
#include <string>
#include <vector>
#include <memory>

#include "db/Database.h"
#include "db/Table.h"
#include "query/Query.h"
#include "query/QueryResult.h"
#include "query/data/SelectQuery.h"
#include "query/data/SwapQuery.h"

namespace {

// Simple helper to drop a table if it exists.
void dropIfExists(const std::string& name) {
  auto& db = Database::getInstance();
  try {
    db.dropTable(name);
  } catch (...) {
    // ignore
  }
}

// Seed Student table (int32-safe values).
void seedStudent() {
  auto tbl = std::make_unique<Table>(
      "Student",
      std::vector<std::string>{"studentID", "class", "totalCredit"});
  tbl->insertByIndex("Bill_Gates", {400812312, 2014, 112});
  tbl->insertByIndex("Steve_Jobs", {400851751, 2014, 115});
  tbl->insertByIndex("Jack_Ma",    {400882382, 2015, 123});
  Database::getInstance().registerTable(std::move(tbl));
}

QueryCondition C(const std::string& f, const std::string& op, const std::string& v) {
  QueryCondition qc; qc.field = f; qc.op = op; qc.value = v; return qc;
}

std::string asString(const QueryResult::Ptr& res) {
  std::ostringstream oss;
  if (res && res->display()) oss << *res;
  return oss.str();
}

// Access integer field by key+field name.
int geti(const Table& t, const std::string& key, const std::string& field) {
  auto obj = const_cast<Table&>(t)[key];
  if (!obj) throw std::runtime_error("row not found: " + key);
  return (*obj)[ t.getFieldIndex(field) ];
}

struct QuerySelectSwapTest : ::testing::Test {
  void SetUp() override {
    dropIfExists("Student");
    seedStudent();
  }
  void TearDown() override {
    dropIfExists("Student");
  }
};

} // namespace

// ---------- SELECT tests ----------

TEST_F(QuerySelectSwapTest, Select_All_NoWhere_PrintsInKeyOrder) {
  // Expect alphabetical order by KEY: Bill_Gates, Jack_Ma, Steve_Jobs
  SelectQuery q("Student", {"KEY", "class", "studentID"}, {});
  auto res = q.execute();
  ASSERT_TRUE(res->success());
  const std::string out = asString(res);
  // If out is empty, your SelectQuery likely prints to cout instead of returning a displayable result.
  ASSERT_FALSE(out.empty()) << "SelectQuery should return a displayable QueryResult (e.g., TextRowsResult).";
  const std::string expect =
      "( Bill_Gates 2014 400812312 )\n"
      "( Jack_Ma 2015 400882382 )\n"
      "( Steve_Jobs 2014 400851751 )\n";
  EXPECT_EQ(out, expect);
}

TEST_F(QuerySelectSwapTest, Select_KeyEquals_FastPath) {
  SelectQuery q("Student", {"KEY", "studentID"}, { C("KEY","=","Steve_Jobs") });
  auto res = q.execute();
  ASSERT_TRUE(res->success());
  const std::string out = asString(res);
  ASSERT_FALSE(out.empty());
  const std::string expect = "( Steve_Jobs 400851751 )\n";
  EXPECT_EQ(out, expect);
}

TEST_F(QuerySelectSwapTest, Select_FieldConditions_AND) {
  // class >= 2014 AND studentID < 450000000
  SelectQuery q("Student", {"KEY", "class", "studentID"},
                { C("class",">=","2014"), C("studentID","<","450000000") });
  auto res = q.execute();
  ASSERT_TRUE(res->success());
  const std::string out = asString(res);
  ASSERT_FALSE(out.empty());
  // All three rows match the predicate above in this dataset.
  const std::string expect =
      "( Bill_Gates 2014 400812312 )\n"
      "( Jack_Ma 2015 400882382 )\n"
      "( Steve_Jobs 2014 400851751 )\n";
  EXPECT_EQ(out, expect);
}

TEST_F(QuerySelectSwapTest, Select_NoMatch_PrintsNothing) {
  SelectQuery q("Student", {"KEY","class"}, { C("class","<","1900") });
  auto res = q.execute();
  ASSERT_TRUE(res->success());
  const std::string out = asString(res);
  EXPECT_TRUE(out.empty());
}

