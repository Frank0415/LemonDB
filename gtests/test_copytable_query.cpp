#include <cstddef>
#include <gtest/gtest.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "db/Database.h"
#include "db/Table.h"
#include "query/QueryResult.h"
#include "query/management/CopyTableQuery.h"

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
    // Intentionally ignore: table may not exist
    (void)0;
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
  Database::getInstance().registerTable(std::move(tbl));
}

int geti(const Table& t, const std::string& key, const std::string& field)
{
  auto obj = const_cast<Table&>(t)[key];
  if (!obj)
    throw std::runtime_error("row not found: " + key);
  return (*obj)[t.getFieldIndex(field)];
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

size_t countRows(const Table& t)
{
  size_t count = 0;
  for (auto it = t.begin(); it != t.end(); ++it)
  {
    ++count;
  }
  return count;
}

struct CopyTableTest : ::testing::Test
{
  void SetUp() override
  {
    dropIfExists("Student");
    dropIfExists("Student_Copy");
    dropIfExists("Employee");
    dropIfExists("Employee_Copy");
    dropIfExists("Backup");
  }
  void TearDown() override
  {
    dropIfExists("Student");
    dropIfExists("Student_Copy");
    dropIfExists("Employee");
    dropIfExists("Employee_Copy");
    dropIfExists("Backup");
  }
};

} // namespace

// Basic copy: source table exists, target doesn't.
TEST_F(CopyTableTest, BasicCopy_Success)
{
  seedStudent();
  CopyTableQuery q("Student", "Student_Copy");
  auto res = q.execute();
  ASSERT_TRUE(res->success());

  // Verify target table was created
  ASSERT_TRUE(tableExists("Student_Copy"));

  auto& db = Database::getInstance();
  const auto& orig = db["Student"];
  const auto& copy = db["Student_Copy"];

  // Verify row count
  EXPECT_EQ(countRows(orig), countRows(copy));
  EXPECT_EQ(countRows(copy), 3);

  // Verify data integrity
  EXPECT_EQ(geti(copy, "Bill_Gates", "studentID"), 400812312);
  EXPECT_EQ(geti(copy, "Bill_Gates", "class"), 2014);
  EXPECT_EQ(geti(copy, "Bill_Gates", "totalCredit"), 112);

  EXPECT_EQ(geti(copy, "Steve_Jobs", "studentID"), 400851751);
  EXPECT_EQ(geti(copy, "Steve_Jobs", "class"), 2014);
  EXPECT_EQ(geti(copy, "Steve_Jobs", "totalCredit"), 115);

  EXPECT_EQ(geti(copy, "Jack_Ma", "studentID"), 400882382);
  EXPECT_EQ(geti(copy, "Jack_Ma", "class"), 2015);
  EXPECT_EQ(geti(copy, "Jack_Ma", "totalCredit"), 123);
}

// Copy with different field structure.
TEST_F(CopyTableTest, DifferentFieldsCopy_Success)
{
  seedEmployee();
  CopyTableQuery q("Employee", "Employee_Copy");
  auto res = q.execute();
  ASSERT_TRUE(res->success());

  ASSERT_TRUE(tableExists("Employee_Copy"));

  auto& db = Database::getInstance();
  const auto& copy = db["Employee_Copy"];

  EXPECT_EQ(countRows(copy), 3);
  EXPECT_EQ(geti(copy, "e001", "age"), 25);
  EXPECT_EQ(geti(copy, "e001", "salary"), 5000);
  EXPECT_EQ(geti(copy, "e001", "dept"), 10);
}

// Target table already exists -> should fail.
TEST_F(CopyTableTest, TargetExists_ShouldFail)
{
  seedStudent();
  seedEmployee();

  CopyTableQuery q("Student", "Employee");
  auto res = q.execute();
  EXPECT_FALSE(res->success());
}

// Source table doesn't exist -> should fail.
TEST_F(CopyTableTest, SourceNotFound_ShouldFail)
{
  CopyTableQuery q("NonExistent", "Backup");
  auto res = q.execute();
  EXPECT_FALSE(res->success());
}

// Empty table copy.
TEST_F(CopyTableTest, EmptyTable_Success)
{
  auto tbl = std::make_unique<Table>("Empty", std::vector<std::string>{"field1", "field2"});
  Database::getInstance().registerTable(std::move(tbl));

  CopyTableQuery q("Empty", "EmptyCopy");
  auto res = q.execute();
  ASSERT_TRUE(res->success());

  ASSERT_TRUE(tableExists("EmptyCopy"));
  auto& db = Database::getInstance();
  const auto& copy = db["EmptyCopy"];
  EXPECT_EQ(countRows(copy), 0);

  dropIfExists("Empty");
  dropIfExists("EmptyCopy");
}

// Original and copy are independent (modify original doesn't affect copy).
TEST_F(CopyTableTest, IndependentCopies_ModifyOriginal)
{
  seedStudent();

  CopyTableQuery q("Student", "Student_Copy");
  auto res = q.execute();
  ASSERT_TRUE(res->success());

  auto& db = Database::getInstance();
  auto& orig = db["Student"];

  // Modify original table
  auto obj = orig["Bill_Gates"];
  ASSERT_NE(obj, nullptr);
  (*obj)[orig.getFieldIndex("totalCredit")] = 999;

  // Verify copy is unchanged
  const auto& copy = db["Student_Copy"];
  EXPECT_EQ(geti(copy, "Bill_Gates", "totalCredit"), 112);
  EXPECT_NE(geti(copy, "Bill_Gates", "totalCredit"), 999);
}

// Copy table with single row.
TEST_F(CopyTableTest, SingleRow_Success)
{
  auto tbl = std::make_unique<Table>("Single", std::vector<std::string>{"value"});
  tbl->insertByIndex("only_one", {42});
  Database::getInstance().registerTable(std::move(tbl));

  CopyTableQuery q("Single", "SingleCopy");
  auto res = q.execute();
  ASSERT_TRUE(res->success());

  auto& db = Database::getInstance();
  const auto& copy = db["SingleCopy"];
  EXPECT_EQ(countRows(copy), 1);
  EXPECT_EQ(geti(copy, "only_one", "value"), 42);

  dropIfExists("Single");
  dropIfExists("SingleCopy");
}

// Copy table with many fields.
TEST_F(CopyTableTest, ManyFields_Success)
{
  auto tbl =
      std::make_unique<Table>("Wide", std::vector<std::string>{"f1", "f2", "f3", "f4", "f5"});
  tbl->insertByIndex("row1", {1, 2, 3, 4, 5});
  tbl->insertByIndex("row2", {10, 20, 30, 40, 50});
  Database::getInstance().registerTable(std::move(tbl));

  CopyTableQuery q("Wide", "WideCopy");
  auto res = q.execute();
  ASSERT_TRUE(res->success());

  auto& db = Database::getInstance();
  const auto& copy = db["WideCopy"];
  EXPECT_EQ(countRows(copy), 2);
  EXPECT_EQ(geti(copy, "row1", "f1"), 1);
  EXPECT_EQ(geti(copy, "row1", "f5"), 5);
  EXPECT_EQ(geti(copy, "row2", "f3"), 30);

  dropIfExists("Wide");
  dropIfExists("WideCopy");
}

// Verify field names are preserved.
TEST_F(CopyTableTest, FieldNamesPreserved)
{
  seedStudent();

  CopyTableQuery q("Student", "Student_Copy");
  auto res = q.execute();
  ASSERT_TRUE(res->success());

  auto& db = Database::getInstance();
  const auto& orig = db["Student"];
  const auto& copy = db["Student_Copy"];

  // Verify field names match
  const auto& origFields = orig.field();
  const auto& copyFields = copy.field();

  ASSERT_EQ(origFields.size(), copyFields.size());
  for (size_t i = 0; i < origFields.size(); ++i)
  {
    EXPECT_EQ(origFields[i], copyFields[i]);
  }
}
