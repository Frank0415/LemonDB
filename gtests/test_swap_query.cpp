#include <gtest/gtest.h>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "db/Database.h"
#include "db/Table.h"
#include "query/QueryResult.h"
#include "query/data/SwapQuery.h"

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
  if (res && res->display())
    oss << *res;
  return oss.str();
}

int geti(const Table& t, const std::string& key, const std::string& field)
{
  auto obj = const_cast<Table&>(t)[key];
  if (!obj)
    throw std::runtime_error("row not found: " + key);
  return (*obj)[t.getFieldIndex(field)];
}

struct SwapTest : ::testing::Test
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

TEST_F(SwapTest, WhereMatches_SwapsTwoRows)
{
  SwapQuery q("Student", {"class", "studentID"}, {C("class", "<", "2015")});
  auto res = q.execute();
  ASSERT_TRUE(res->success());
  const std::string out = asString(res);
  EXPECT_NE(out.find("Affected 2 rows."), std::string::npos);

  const auto& stu = Database::getInstance()["Student"];
  EXPECT_EQ(geti(stu, "Bill_Gates", "studentID"), 2014);
  EXPECT_EQ(geti(stu, "Bill_Gates", "class"), 400812312);
  EXPECT_EQ(geti(stu, "Steve_Jobs", "studentID"), 2014);
  EXPECT_EQ(geti(stu, "Steve_Jobs", "class"), 400851751);
  EXPECT_EQ(geti(stu, "Jack_Ma", "studentID"), 400882382);
  EXPECT_EQ(geti(stu, "Jack_Ma", "class"), 2015);
}

TEST_F(SwapTest, NoMatch_AffectedZero)
{
  SwapQuery q("Student", {"class", "studentID"}, {C("class", "<", "1900")});
  auto res = q.execute();
  ASSERT_TRUE(res->success());
  const std::string out = asString(res);
  EXPECT_NE(out.find("Affected 0 rows."), std::string::npos);

  const auto& stu = Database::getInstance()["Student"];
  EXPECT_EQ(geti(stu, "Bill_Gates", "class"), 2014);
  EXPECT_EQ(geti(stu, "Steve_Jobs", "class"), 2014);
  EXPECT_EQ(geti(stu, "Jack_Ma", "class"), 2015);
}

TEST_F(SwapTest, SameField_NoEffect)
{
  SwapQuery q("Student", {"class", "class"}, {});
  auto res = q.execute();
  ASSERT_TRUE(res->success());
  const std::string out = asString(res);
  EXPECT_NE(out.find("Affected 0 rows."), std::string::npos);
}

TEST_F(SwapTest, FullTableSwap_ThenStateAsExpected)
{
  SwapQuery q("Student", {"class", "studentID"}, {});
  auto res = q.execute();
  ASSERT_TRUE(res->success());
  const std::string out = asString(res);
  EXPECT_NE(out.find("Affected 3 rows."), std::string::npos);

  const auto& stu = Database::getInstance()["Student"];
  EXPECT_EQ(geti(stu, "Bill_Gates", "studentID"), 2014);
  EXPECT_EQ(geti(stu, "Bill_Gates", "class"), 400812312);
  EXPECT_EQ(geti(stu, "Jack_Ma", "studentID"), 2015);
  EXPECT_EQ(geti(stu, "Jack_Ma", "class"), 400882382);
  EXPECT_EQ(geti(stu, "Steve_Jobs", "studentID"), 2014);
  EXPECT_EQ(geti(stu, "Steve_Jobs", "class"), 400851751);
}
