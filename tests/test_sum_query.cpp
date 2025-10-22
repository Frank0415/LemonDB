#include <gtest/gtest.h>
#include <sstream>
#include <string>
#include <vector>
#include <memory>

#include "db/Database.h"
#include "db/Table.h"
#include "query/QueryResult.h"
#include "query/data/SumQuery.h"

namespace {

void dropIfExists(const std::string& name) {
  auto& db = Database::getInstance();
  try { db.dropTable(name); } catch (...) {}
}

void seedStudent() {
  auto tbl = std::make_unique<Table>(
    "Student",
    std::vector<std::string>{"studentID", "class", "totalCredit"}
  );
  // KEY, studentID, class, totalCredit  (int32-safe)
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
  if (res && res->success()) oss << *res;
  return oss.str();
}

struct SumTest : ::testing::Test {
  void SetUp() override {
    dropIfExists("Student");
    seedStudent();
  }
  void TearDown() override {
    dropIfExists("Student");
  }
};

} // namespace

// Multi-field sum across whole table.
TEST_F(SumTest, MultiField_WholeTable) {
  // Expected: totalCredit = 112+115+123 = 350, class = 2014+2014+2015 = 6043
  SumQuery q("Student", {"totalCredit", "class"}, {});
  auto res = q.execute();
  ASSERT_TRUE(res->success());
  const std::string out = asString(res);
  EXPECT_EQ(out, "ANSWER = ( 350 6043 )\n");
}

// KEY fast path: only the matched row contributes.
TEST_F(SumTest, MultiField_KeyEqualsFastPath) {
  // Row: Steve_Jobs -> totalCredit=115, class=2014
  SumQuery q("Student", {"totalCredit", "class"}, { C("KEY","=","Steve_Jobs") });
  auto res = q.execute();
  ASSERT_TRUE(res->success());
  const std::string out = asString(res);
  EXPECT_EQ(out, "ANSWER = ( 115 2014 )\n");
}

// No match -> zeros vector.
TEST_F(SumTest, MultiField_NoMatch_ReturnsZeros) {
  SumQuery q("Student", {"totalCredit", "class"}, { C("class","<","1900") });
  auto res = q.execute();
  ASSERT_TRUE(res->success());
  const std::string out = asString(res);
  EXPECT_EQ(out, "ANSWER = ( 0 0 )\n");
}

// Using KEY as an operand should fail.
TEST_F(SumTest, UsingKEYAsOperand_IsError) {
  SumQuery q("Student", {"KEY"}, {});
  auto res = q.execute();
  EXPECT_FALSE(res->success());
}

// Unknown field should fail.
TEST_F(SumTest, UnknownField_IsError) {
  SumQuery q("Student", {"doesNotExist"}, {});
  auto res = q.execute();
  EXPECT_FALSE(res->success());
}

// Empty operand list should fail (if you enforce >=1 operand).
TEST_F(SumTest, EmptyOperand_IsError) {
  SumQuery q("Student", {}, {});
  auto res = q.execute();
  EXPECT_FALSE(res->success());
}