#include <gtest/gtest.h>
#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "db/Database.h"
#include "db/Table.h"
#include "query/QueryResult.h"
#include "query/management/TruncateTableQuery.h"

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

void seedEmployee()
{
  auto tbl = std::make_unique<Table>("Employee", std::vector<std::string>{"age", "salary", "dept"});
  tbl->insertByIndex("e001", {25, 5000, 10});
  tbl->insertByIndex("e002", {40, 12000, 20});
  tbl->insertByIndex("e003", {25, 7000, 30});
  tbl->insertByIndex("e004", {31, 8800, 20});
  tbl->insertByIndex("e005", {28, 6500, 10});
  Database::getInstance().registerTable(std::move(tbl));
}

size_t countRows(const Table& t)
{
  size_t count = 0;
  for (auto it = t.begin(); it != t.end(); ++it)
  {
    ++count;
  }
  return count;
}

bool tableExists(const std::string& name)
{
  auto& db = Database::getInstance();
  try
  {
    (void)db[name];
    return true;
  }
  catch (...)
  {
    return false;
  }
}

struct TruncateTest : ::testing::Test
{
  void SetUp() override
  {
    dropIfExists("Student");
    dropIfExists("Employee");
    dropIfExists("Empty");
  }
  void TearDown() override
  {
    dropIfExists("Student");
    dropIfExists("Employee");
    dropIfExists("Empty");
  }
};

} // namespace

// Basic truncate: table with data should become empty.
TEST_F(TruncateTest, BasicTruncate_Success)
{
  seedStudent();
  auto& db = Database::getInstance();
  const auto& table = db["Student"];

  // Verify table has data before truncate
  ASSERT_EQ(countRows(table), 3);

  TruncateTableQuery q("Student");
  auto res = q.execute();
  ASSERT_TRUE(res->success());

  // Verify table is empty after truncate
  EXPECT_EQ(countRows(table), 0);

  // Verify table still exists
  EXPECT_TRUE(tableExists("Student"));
}

// Truncate table with more rows.
TEST_F(TruncateTest, TruncateEmployeeTable_Success)
{
  seedEmployee();
  auto& db = Database::getInstance();
  const auto& table = db["Employee"];

  ASSERT_EQ(countRows(table), 5);

  TruncateTableQuery q("Employee");
  auto res = q.execute();
  ASSERT_TRUE(res->success());

  EXPECT_EQ(countRows(table), 0);
  EXPECT_TRUE(tableExists("Employee"));
}

// Truncate empty table should succeed.
TEST_F(TruncateTest, TruncateEmptyTable_Success)
{
  auto tbl = std::make_unique<Table>("Empty", std::vector<std::string>{"field1", "field2"});
  Database::getInstance().registerTable(std::move(tbl));

  auto& db = Database::getInstance();
  const auto& table = db["Empty"];
  ASSERT_EQ(countRows(table), 0);

  TruncateTableQuery q("Empty");
  auto res = q.execute();
  ASSERT_TRUE(res->success());

  EXPECT_EQ(countRows(table), 0);
  EXPECT_TRUE(tableExists("Empty"));
}

// Truncate non-existent table should fail.
TEST_F(TruncateTest, TruncateNonExistent_ShouldFail)
{
  TruncateTableQuery q("NonExistent");
  auto res = q.execute();
  EXPECT_FALSE(res->success());
}

// After truncate, table structure (fields) should be preserved.
TEST_F(TruncateTest, FieldsPreservedAfterTruncate)
{
  seedStudent();
  auto& db = Database::getInstance();
  const auto& table = db["Student"];

  // Get field names before truncate
  const auto& fieldsBefore = table.field();
  std::vector<std::string> expectedFields = {"studentID", "class", "totalCredit"};

  ASSERT_EQ(fieldsBefore.size(), expectedFields.size());
  for (size_t i = 0; i < expectedFields.size(); ++i)
  {
    EXPECT_EQ(fieldsBefore[i], expectedFields[i]);
  }

  TruncateTableQuery q("Student");
  auto res = q.execute();
  ASSERT_TRUE(res->success());

  // Get field names after truncate
  const auto& fieldsAfter = table.field();
  ASSERT_EQ(fieldsAfter.size(), expectedFields.size());
  for (size_t i = 0; i < expectedFields.size(); ++i)
  {
    EXPECT_EQ(fieldsAfter[i], expectedFields[i]);
  }
}

// Can insert into table after truncate.
TEST_F(TruncateTest, CanInsertAfterTruncate)
{
  seedStudent();
  auto& db = Database::getInstance();
  auto& table = db["Student"];

  TruncateTableQuery q("Student");
  auto res = q.execute();
  ASSERT_TRUE(res->success());
  ASSERT_EQ(countRows(table), 0);

  // Try to insert new data
  table.insertByIndex("Elon_Musk", {400900000, 2020, 100});

  EXPECT_EQ(countRows(table), 1);

  // Verify the inserted data
  auto obj = table["Elon_Musk"];
  ASSERT_NE(obj, nullptr);
  EXPECT_EQ((*obj)[table.getFieldIndex("studentID")], 400900000);
  EXPECT_EQ((*obj)[table.getFieldIndex("class")], 2020);
  EXPECT_EQ((*obj)[table.getFieldIndex("totalCredit")], 100);
}

// Multiple truncates should work.
TEST_F(TruncateTest, MultipleTruncates_Success)
{
  seedStudent();
  auto& db = Database::getInstance();
  const auto& table = db["Student"];

  // First truncate
  TruncateTableQuery q1("Student");
  auto res1 = q1.execute();
  ASSERT_TRUE(res1->success());
  EXPECT_EQ(countRows(table), 0);

  // Second truncate (on already empty table)
  TruncateTableQuery q2("Student");
  auto res2 = q2.execute();
  ASSERT_TRUE(res2->success());
  EXPECT_EQ(countRows(table), 0);
}

// Truncate then repopulate then truncate again.
TEST_F(TruncateTest, TruncateRepopulateTruncate)
{
  seedStudent();
  auto& db = Database::getInstance();
  auto& table = db["Student"];

  ASSERT_EQ(countRows(table), 3);

  // First truncate
  TruncateTableQuery q1("Student");
  auto res1 = q1.execute();
  ASSERT_TRUE(res1->success());
  EXPECT_EQ(countRows(table), 0);

  // Repopulate
  table.insertByIndex("New_Person1", {100, 2021, 50});
  table.insertByIndex("New_Person2", {200, 2022, 60});
  EXPECT_EQ(countRows(table), 2);

  // Second truncate
  TruncateTableQuery q2("Student");
  auto res2 = q2.execute();
  ASSERT_TRUE(res2->success());
  EXPECT_EQ(countRows(table), 0);
}

// Truncate table with single row.
TEST_F(TruncateTest, TruncateSingleRow_Success)
{
  auto tbl = std::make_unique<Table>("Single", std::vector<std::string>{"value"});
  tbl->insertByIndex("only_one", {42});
  Database::getInstance().registerTable(std::move(tbl));

  auto& db = Database::getInstance();
  const auto& table = db["Single"];
  ASSERT_EQ(countRows(table), 1);

  TruncateTableQuery q("Single");
  auto res = q.execute();
  ASSERT_TRUE(res->success());

  EXPECT_EQ(countRows(table), 0);

  dropIfExists("Single");
}

// Truncate table with many fields.
TEST_F(TruncateTest, TruncateManyFields_Success)
{
  auto tbl = std::make_unique<Table>(
      "Wide", std::vector<std::string>{"f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8"});
  tbl->insertByIndex("row1", {1, 2, 3, 4, 5, 6, 7, 8});
  tbl->insertByIndex("row2", {10, 20, 30, 40, 50, 60, 70, 80});
  tbl->insertByIndex("row3", {100, 200, 300, 400, 500, 600, 700, 800});
  Database::getInstance().registerTable(std::move(tbl));

  auto& db = Database::getInstance();
  const auto& table = db["Wide"];
  ASSERT_EQ(countRows(table), 3);

  TruncateTableQuery q("Wide");
  auto res = q.execute();
  ASSERT_TRUE(res->success());

  EXPECT_EQ(countRows(table), 0);
  EXPECT_EQ(table.field().size(), 8);

  dropIfExists("Wide");
}

// Verify table name is preserved after truncate.
TEST_F(TruncateTest, TableNamePreserved)
{
  seedEmployee();
  auto& db = Database::getInstance();

  TruncateTableQuery q("Employee");
  auto res = q.execute();
  ASSERT_TRUE(res->success());

  // Should still be able to access by same name
  EXPECT_TRUE(tableExists("Employee"));
  const auto& table = db["Employee"];
  EXPECT_EQ(countRows(table), 0);
}
