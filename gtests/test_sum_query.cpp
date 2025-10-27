#include <gtest/gtest.h>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "db/Database.h"
#include "db/Table.h"
#include "query/QueryResult.h"
#include "query/data/SumQuery.h"

namespace
{

void dropIfExists(const std::string& name)
{
  auto& db = Database::getInstance();
  try
  {
    db.dropTable(name);
  }
  catch (...)
  {
  }
}

void seedStudent()
{
  auto tbl = std::make_unique<Table>("Student",
                                     std::vector<std::string>{"studentID", "class", "totalCredit"});
  // KEY, studentID, class, totalCredit  (int32-safe)
  tbl->insertByIndex("Bill_Gates", {400812312, 2014, 112});
  tbl->insertByIndex("Steve_Jobs", {400851751, 2014, 115});
  tbl->insertByIndex("Jack_Ma", {400882382, 2015, 123});
  Database::getInstance().registerTable(std::move(tbl));
}

QueryCondition C(const std::string& f, const std::string& op, const std::string& v)
{
  QueryCondition qc;
  qc.field = f;
  qc.op = op;
  qc.value = v;
  return qc;
}

std::string asString(const QueryResult::Ptr& res)
{
  std::ostringstream oss;
  if (res && res->success())
    oss << *res;
  return oss.str();
}

struct SumTest : ::testing::Test
{
  void SetUp() override
  {
    dropIfExists("Student");
    seedStudent();
  }
  void TearDown() override
  {
    dropIfExists("Student");
  }
};

} // namespace

// Multi-field sum across whole table.
TEST_F(SumTest, MultiField_WholeTable)
{
  // Expected: totalCredit = 112+115+123 = 350, class = 2014+2014+2015 = 6043
  SumQuery q("Student", {"totalCredit", "class"}, {});
  auto res = q.execute();
  ASSERT_TRUE(res->success());
  const std::string out = asString(res);
  EXPECT_EQ(out, "ANSWER = ( 350 6043 )\n");
}

// KEY fast path: only the matched row contributes.
TEST_F(SumTest, MultiField_KeyEqualsFastPath)
{
  // Row: Steve_Jobs -> totalCredit=115, class=2014
  SumQuery q("Student", {"totalCredit", "class"}, {C("KEY", "=", "Steve_Jobs")});
  auto res = q.execute();
  ASSERT_TRUE(res->success());
  const std::string out = asString(res);
  EXPECT_EQ(out, "ANSWER = ( 115 2014 )\n");
}

// No match -> zeros vector.
TEST_F(SumTest, MultiField_NoMatch_ReturnsZeros)
{
  SumQuery q("Student", {"totalCredit", "class"}, {C("class", "<", "1900")});
  auto res = q.execute();
  ASSERT_TRUE(res->success());
  const std::string out = asString(res);
  EXPECT_EQ(out, "ANSWER = ( 0 0 )\n");
}

// Using KEY as an operand should fail.
TEST_F(SumTest, UsingKEYAsOperand_IsError)
{
  SumQuery q("Student", {"KEY"}, {});
  auto res = q.execute();
  EXPECT_FALSE(res->success());
}

// Unknown field should fail.
TEST_F(SumTest, UnknownField_IsError)
{
  SumQuery q("Student", {"doesNotExist"}, {});
  auto res = q.execute();
  EXPECT_FALSE(res->success());
}

// Empty operand list should fail (if you enforce >=1 operand).
TEST_F(SumTest, EmptyOperand_IsError)
{
  SumQuery q("Student", {}, {});
  auto res = q.execute();
  EXPECT_FALSE(res->success());
}

// Single field sum across whole table.
TEST_F(SumTest, SingleField_WholeTable)
{
  // Expected: totalCredit = 112+115+123 = 350
  SumQuery q("Student", {"totalCredit"}, {});
  auto res = q.execute();
  ASSERT_TRUE(res->success());
  const std::string out = asString(res);
  EXPECT_EQ(out, "ANSWER = ( 350 )\n");
}

// Sum with single WHERE condition (equality).
TEST_F(SumTest, SingleCondition_Equality)
{
  // Only class=2014: Bill_Gates(112) + Steve_Jobs(115) = 227
  SumQuery q("Student", {"totalCredit"}, {C("class", "=", "2014")});
  auto res = q.execute();
  ASSERT_TRUE(res->success());
  const std::string out = asString(res);
  EXPECT_EQ(out, "ANSWER = ( 227 )\n");
}

// Sum with single WHERE condition (greater than).
TEST_F(SumTest, SingleCondition_GreaterThan)
{
  // totalCredit > 112: Steve_Jobs(115) + Jack_Ma(123) = 238
  SumQuery q("Student", {"totalCredit"}, {C("totalCredit", ">", "112")});
  auto res = q.execute();
  ASSERT_TRUE(res->success());
  const std::string out = asString(res);
  EXPECT_EQ(out, "ANSWER = ( 238 )\n");
}

// Sum with single WHERE condition (less than).
TEST_F(SumTest, SingleCondition_LessThan)
{
  // totalCredit < 120: Bill_Gates(112) + Steve_Jobs(115) = 227
  SumQuery q("Student", {"totalCredit"}, {C("totalCredit", "<", "120")});
  auto res = q.execute();
  ASSERT_TRUE(res->success());
  const std::string out = asString(res);
  EXPECT_EQ(out, "ANSWER = ( 227 )\n");
}

// Sum with single WHERE condition (greater than or equal).
TEST_F(SumTest, SingleCondition_GreaterEqual)
{
  // totalCredit >= 115: Steve_Jobs(115) + Jack_Ma(123) = 238
  SumQuery q("Student", {"totalCredit"}, {C("totalCredit", ">=", "115")});
  auto res = q.execute();
  ASSERT_TRUE(res->success());
  const std::string out = asString(res);
  EXPECT_EQ(out, "ANSWER = ( 238 )\n");
}

// Sum with single WHERE condition (less than or equal).
TEST_F(SumTest, SingleCondition_LessEqual)
{
  // totalCredit <= 115: Bill_Gates(112) + Steve_Jobs(115) = 227
  SumQuery q("Student", {"totalCredit"}, {C("totalCredit", "<=", "115")});
  auto res = q.execute();
  ASSERT_TRUE(res->success());
  const std::string out = asString(res);
  EXPECT_EQ(out, "ANSWER = ( 227 )\n");
}

// Sum with multiple WHERE conditions (AND logic).
TEST_F(SumTest, MultipleConditions_AND)
{
  // class=2014 AND totalCredit>112: only Steve_Jobs(115)
  SumQuery q("Student", {"totalCredit"}, {C("class", "=", "2014"), C("totalCredit", ">", "112")});
  auto res = q.execute();
  ASSERT_TRUE(res->success());
  const std::string out = asString(res);
  EXPECT_EQ(out, "ANSWER = ( 115 )\n");
}

// Sum with multiple WHERE conditions (no match).
TEST_F(SumTest, MultipleConditions_NoMatch)
{
  // class=2014 AND totalCredit>120: no match
  SumQuery q("Student", {"totalCredit"}, {C("class", "=", "2014"), C("totalCredit", ">", "120")});
  auto res = q.execute();
  ASSERT_TRUE(res->success());
  const std::string out = asString(res);
  EXPECT_EQ(out, "ANSWER = ( 0 )\n");
}

// Sum all three fields.
TEST_F(SumTest, AllFields)
{
  // studentID sum, class sum, totalCredit sum
  // studentID: 400812312 + 400851751 + 400882382 = 1202546445
  // class: 2014 + 2014 + 2015 = 6043
  // totalCredit: 112 + 115 + 123 = 350
  SumQuery q("Student", {"studentID", "class", "totalCredit"}, {});
  auto res = q.execute();
  ASSERT_TRUE(res->success());
  const std::string out = asString(res);
  EXPECT_EQ(out, "ANSWER = ( 1202546445 6043 350 )\n");
}

// Sum with KEY condition that matches exactly one row.
TEST_F(SumTest, KeyCondition_SingleMatch)
{
  // Jack_Ma: totalCredit=123, class=2015
  SumQuery q("Student", {"totalCredit", "class"}, {C("KEY", "=", "Jack_Ma")});
  auto res = q.execute();
  ASSERT_TRUE(res->success());
  const std::string out = asString(res);
  EXPECT_EQ(out, "ANSWER = ( 123 2015 )\n");
}

// Sum with KEY condition that doesn't match.
TEST_F(SumTest, KeyCondition_NoMatch)
{
  // Non-existent key
  SumQuery q("Student", {"totalCredit"}, {C("KEY", "=", "NonExistent")});
  auto res = q.execute();
  ASSERT_TRUE(res->success());
  const std::string out = asString(res);
  EXPECT_EQ(out, "ANSWER = ( 0 )\n");
}

// Sum with condition on different field than summed field.
TEST_F(SumTest, ConditionOnDifferentField)
{
  // Sum studentID where class=2014
  // Bill_Gates(400812312) + Steve_Jobs(400851751) = 801664063
  SumQuery q("Student", {"studentID"}, {C("class", "=", "2014")});
  auto res = q.execute();
  ASSERT_TRUE(res->success());
  const std::string out = asString(res);
  EXPECT_EQ(out, "ANSWER = ( 801664063 )\n");
}

// Sum on non-existent table should fail.
TEST_F(SumTest, NonExistentTable_IsError)
{
  SumQuery q("NonExistentTable", {"totalCredit"}, {});
  auto res = q.execute();
  EXPECT_FALSE(res->success());
}

// Sum with boundary value condition (exact match on boundary).
TEST_F(SumTest, BoundaryValue_ExactMatch)
{
  // totalCredit = 115: only Steve_Jobs
  SumQuery q("Student", {"totalCredit", "class"}, {C("totalCredit", "=", "115")});
  auto res = q.execute();
  ASSERT_TRUE(res->success());
  const std::string out = asString(res);
  EXPECT_EQ(out, "ANSWER = ( 115 2014 )\n");
}

// Sum with fields in reverse order.
TEST_F(SumTest, ReverseFieldOrder)
{
  // Verify order matters: class first, then totalCredit
  SumQuery q("Student", {"class", "totalCredit"}, {});
  auto res = q.execute();
  ASSERT_TRUE(res->success());
  const std::string out = asString(res);
  EXPECT_EQ(out, "ANSWER = ( 6043 350 )\n");
}

// Sum same field multiple times (if allowed).
TEST_F(SumTest, SameFieldMultipleTimes)
{
  // Sum totalCredit twice: should give same result twice
  SumQuery q("Student", {"totalCredit", "totalCredit"}, {});
  auto res = q.execute();
  ASSERT_TRUE(res->success());
  const std::string out = asString(res);
  EXPECT_EQ(out, "ANSWER = ( 350 350 )\n");
}
