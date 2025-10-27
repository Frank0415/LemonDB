// FILE: tests/test_count_query.cpp
#include <gtest/gtest.h>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "db/Database.h"
#include "db/Table.h"
#include "query/Query.h"
#include "query/QueryResult.h"
#include "query/data/CountQuery.h"

// Helper namespace for test setup
namespace
{

// Drops a table if it exists to ensure a clean state for each test.
void dropIfExists(const std::string& name)
{
  auto& db = Database::getInstance();
  try
  {
    db.dropTable(name);
  }
  catch (...)
  {
    // Ignore if the table doesn't exist
  }
}

// Seeds the test database with a table similar to the 'Student' table.
void seedStudentCountTable()
{
  auto tbl = std::make_unique<Table>("StudentCountTest",
                                     std::vector<std::string>{"studentID", "class", "totalCredit"});
  // Data matching the example in p2.pdf
  tbl->insertByIndex("Bill_Gates", {400812312, 2014, 112});
  tbl->insertByIndex("Steve_Jobs", {400851751, 2014, 115});
  tbl->insertByIndex("Jack_Ma", {400882382, 2015, 123});
  Database::getInstance().registerTable(std::move(tbl));
}

// Helper to create a QueryCondition struct concisely.
QueryCondition C(const std::string& field, const std::string& op, const std::string& value)
{
  QueryCondition qc;
  qc.field = field;
  qc.fieldId = 0;
  qc.op = op;
  qc.value = value;
  qc.valueParsed = 0;
  return qc;
}

// Helper to convert a QueryResult pointer to a string for easy comparison.
std::string asString(const QueryResult::Ptr& res)
{
  std::ostringstream oss;
  if (res && res->display())
  {
    oss << *res;
  }
  return oss.str();
}

// Test fixture for CountQuery tests.
class CountQueryTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    // Ensure a clean state before each test
    dropIfExists("StudentCountTest");
    // Seed the database with our test table
    seedStudentCountTable();
  }

  void TearDown() override
  {
    // Clean up the test table after each test
    dropIfExists("StudentCountTest");
  }
};

} // namespace

// Test case 1: Count all records in a table (no WHERE clause).
// Corresponds to: COUNT () FROM Student;
TEST_F(CountQueryTest, CountAllRecords)
{
  // Create a CountQuery with an empty operand and condition list.
  CountQuery q("StudentCountTest", {}, {});
  auto res = q.execute();

  // The query should succeed.
  ASSERT_TRUE(res->success());

  // The output should match the format specified in p2.pdf.
  const std::string out = asString(res);
  EXPECT_EQ(out, "ANSWER = 3\n");
}

// Test case 2: Count records matching a specific condition.
// Corresponds to: COUNT () FROM Student WHERE (class < 2015);
TEST_F(CountQueryTest, CountWithCondition)
{
  // Create a CountQuery with a condition: class < 2015.
  CountQuery q("StudentCountTest", {}, {C("class", "<", "2015")});
  auto res = q.execute();

  // The query should succeed.
  ASSERT_TRUE(res->success());

  // Two records match this condition (Bill_Gates and Steve_Jobs).
  const std::string out = asString(res);
  EXPECT_EQ(out, "ANSWER = 2\n");
}

// Test case 3: Count when no records match the condition.
TEST_F(CountQueryTest, CountWithNoMatchingRecords)
{
  // Create a CountQuery with a condition that matches no records.
  CountQuery q("StudentCountTest", {}, {C("class", ">", "2020")});
  auto res = q.execute();

  // The query should succeed.
  ASSERT_TRUE(res->success());

  // The count should be zero.
  const std::string out = asString(res);
  EXPECT_EQ(out, "ANSWER = 0\n");
}

// Test case 4: Attempting to count on a non-existent table.
TEST_F(CountQueryTest, CountOnNonExistentTable)
{
  // Create a CountQuery targeting a table that does not exist.
  CountQuery q("NonExistentTable", {}, {});
  auto res = q.execute();

  // The query should fail.
  EXPECT_FALSE(res->success());
}

// Test case 5: Count query with operands should fail.
// The syntax is `COUNT ()`, so operands are not allowed.
TEST_F(CountQueryTest, CountWithOperandsFails)
{
  // Create a CountQuery with an invalid operand.
  CountQuery q("StudentCountTest", {"some_operand"}, {});
  auto res = q.execute();

  // The query should fail because COUNT does not take operands.
  EXPECT_FALSE(res->success());
}

// Test case 6: Multiple AND conditions.
TEST_F(CountQueryTest, CountWithMultipleAndConditions)
{
  // COUNT () FROM table WHERE (class = 2014) AND (totalCredit > 112)
  // This should match only Steve_Jobs.
  CountQuery q("StudentCountTest", {}, {C("class", "=", "2014"), C("totalCredit", ">", "112")});
  auto res = q.execute();

  ASSERT_TRUE(res->success());
  const std::string out = asString(res);
  EXPECT_EQ(out, "ANSWER = 1\n");
}
