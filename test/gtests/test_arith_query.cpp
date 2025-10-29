#include <gtest/gtest.h>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "../src/db/Database.h"
#include "../src/db/Table.h"
#include "../src/query/Query.h"
#include "../src/query/data/AddQuery.h"
#include "../src/query/data/SubQuery.h"

TEST(AddQueryTest, AddSumsFieldsIntoDestination)
{
  using TableV = Table::ValueType;
  // create table and register
  auto t = std::make_unique<Table>("add_test", std::vector<std::string>{"f1", "f2", "sum"});
  Table& table = Database::getInstance().registerTable(std::move(t));

  // insert rows
  table.insertByIndex("k1", std::vector<TableV>{1, 2, 0});
  table.insertByIndex("k2", std::vector<TableV>{3, 4, 0});
  table.insertByIndex("k3", std::vector<TableV>{5, 6, 0});

  // prepare operands: ADD ( f1 f2 sum ) - sum f1 and f2 into sum field
  std::vector<std::string> operands = {"f1", "f2", "sum"};
  std::vector<QueryCondition> conds; // no WHERE

  auto q = std::make_unique<AddQuery>("add_test", operands, conds);
  auto r = q->execute();
  EXPECT_TRUE(r->success());
  std::ostringstream oss;
  oss << *r;
  EXPECT_EQ(oss.str(), "Affected 3 rows.\n"); // 3 rows affected

  // verify results
  EXPECT_EQ((*table["k1"])[2], 3);  // 1 + 2 = 3
  EXPECT_EQ((*table["k2"])[2], 7);  // 3 + 4 = 7
  EXPECT_EQ((*table["k3"])[2], 11); // 5 + 6 = 11

  // clean up
  Database::getInstance().dropTable("add_test");
}

TEST(AddQueryTest, AddWithCondition)
{
  using TableV = Table::ValueType;
  // create table and register
  auto t = std::make_unique<Table>("add_cond_test", std::vector<std::string>{"f1", "f2", "sum"});
  Table& table = Database::getInstance().registerTable(std::move(t));

  // insert rows
  table.insertByIndex("k1", std::vector<TableV>{1, 2, 0});
  table.insertByIndex("k2", std::vector<TableV>{3, 4, 0});
  table.insertByIndex("k3", std::vector<TableV>{5, 6, 0});

  // prepare operands and condition: ADD ( f1 f2 sum ) WHERE ( f1 > 2 )
  std::vector<std::string> operands = {"f1", "f2", "sum"};
  QueryCondition cond;
  cond.field = "f1";
  cond.fieldId = 0;
  cond.op = ">";
  cond.value = "2";
  cond.valueParsed = 0;
  std::vector<QueryCondition> conds{cond};

  auto q = std::make_unique<AddQuery>("add_cond_test", operands, conds);
  auto r = q->execute();
  EXPECT_TRUE(r->success());
  std::ostringstream oss;
  oss << *r;
  EXPECT_EQ(oss.str(), "Affected 2 rows.\n"); // 1 row affected (k3)

  // verify results
  EXPECT_EQ((*table["k1"])[2], 0);  // unchanged
  EXPECT_EQ((*table["k2"])[2], 7);  // 3 + 4 = 7
  EXPECT_EQ((*table["k3"])[2], 11); // 5 + 6 = 11

  // clean up
  Database::getInstance().dropTable("add_cond_test");
}

TEST(SubQueryTest, DelSubFieldsIntoDestination)
{
  using TableV = Table::ValueType;
  // create table and register
  auto t = std::make_unique<Table>("sub_test", std::vector<std::string>{"f1", "f2", "diff"});
  Table& table = Database::getInstance().registerTable(std::move(t));

  // insert rows
  table.insertByIndex("k1", std::vector<TableV>{5, 2, 0});
  table.insertByIndex("k2", std::vector<TableV>{10, 4, 0});
  table.insertByIndex("k3", std::vector<TableV>{15, 6, 0});

  // prepare operands: SUB ( f1 f2 diff ) - subtract f2 from f1 into diff field
  std::vector<std::string> operands = {"f1", "f2", "diff"};
  std::vector<QueryCondition> conds; // no WHERE

  auto q = std::make_unique<SubQuery>("sub_test", operands, conds);
  auto r = q->execute();
  EXPECT_TRUE(r->success());
  std::ostringstream oss;
  oss << *r;
  EXPECT_EQ(oss.str(), "Affected 3 rows.\n"); // 3 rows affected

  // verify results
  EXPECT_EQ((*table["k1"])[2], 3); // 5 - 2 = 3
  EXPECT_EQ((*table["k2"])[2], 6); // 10 - 4 = 6
  EXPECT_EQ((*table["k3"])[2], 9); // 15 - 6 = 9

  // clean up
  Database::getInstance().dropTable("sub_test");
}

TEST(SubQueryTest, SubWithCondition)
{
  using TableV = Table::ValueType;
  // create table and register
  auto t = std::make_unique<Table>("sub_cond_test", std::vector<std::string>{"f1", "f2", "diff"});
  Table& table = Database::getInstance().registerTable(std::move(t));

  // insert rows
  table.insertByIndex("k1", std::vector<TableV>{5, 2, 0});
  table.insertByIndex("k2", std::vector<TableV>{10, 4, 0});
  table.insertByIndex("k3", std::vector<TableV>{15, 6, 0});

  // prepare operands and condition: SUB ( f1 f2 diff ) WHERE ( f1 >= 10 )
  std::vector<std::string> operands = {"f1", "f2", "diff"};
  QueryCondition cond;
  cond.field = "f1";
  cond.fieldId = 0;
  cond.op = ">=";
  cond.value = "10";
  cond.valueParsed = 0;
  std::vector<QueryCondition> conds{cond};

  auto q = std::make_unique<SubQuery>("sub_cond_test", operands, conds);
  auto r = q->execute();
  EXPECT_TRUE(r->success());
  std::ostringstream oss;
  oss << *r;
  EXPECT_EQ(oss.str(), "Affected 2 rows.\n"); // 2 rows affected (k2, k3)

  // verify results
  EXPECT_EQ((*table["k1"])[2], 0); // unchanged
  EXPECT_EQ((*table["k2"])[2], 6); // 10 - 4 = 6
  EXPECT_EQ((*table["k3"])[2], 9); // 15 - 6 = 9

  // clean up
  Database::getInstance().dropTable("sub_cond_test");
}

TEST(AddSubQueryComplicatedTest, AddSubCopiesTotalCreditToStudentID)
{
  using TableV = Table::ValueType;
  // create table like Student: fields studentID, class, totalCredit
  auto t = std::make_unique<Table>("student_like",
                                   std::vector<std::string>{"studentID", "class", "totalCredit"});
  Table& table = Database::getInstance().registerTable(std::move(t));

  // insert rows similar to student.tbl
  table.insertByIndex("s1", std::vector<TableV>{123123, 2014, 112});
  table.insertByIndex("s2", std::vector<TableV>{517517, 2014, 115});
  table.insertByIndex("s3", std::vector<TableV>{823823, 2015, 123});
  table.insertByIndex("s4", std::vector<TableV>{66666, 2015, 120});
  table.insertByIndex("s5", std::vector<TableV>{777777, 2016, 130});

  // prepare operands: ADD ( totalCredit studentID ) - copy totalCredit to
  // studentID
  std::vector<std::string> operands = {"totalCredit", "studentID"};

  // conditions: totalCredit > 100 AND class < 2015
  QueryCondition cond1;
  cond1.field = "totalCredit";
  cond1.fieldId = 0;
  cond1.op = ">";
  cond1.value = "100";
  cond1.valueParsed = 0;

  QueryCondition cond2;
  cond2.field = "class";
  cond2.fieldId = 0;
  cond2.op = "<";
  cond2.value = "2015";
  cond2.valueParsed = 0;

  std::vector<QueryCondition> conds{cond1, cond2};

  auto q = std::make_unique<AddQuery>("student_like", operands, conds);
  auto r = q->execute();
  EXPECT_TRUE(r->success());
  std::ostringstream oss;
  oss << *r;
  EXPECT_EQ(oss.str(), "Affected 2 rows.\n"); // s1 and s2 match

  // verify results: for s1 and s2, studentID field should be set to totalCredit
  EXPECT_EQ((*table["s1"])[0], 112); // studentID set to totalCredit
  EXPECT_EQ((*table["s2"])[0], 115); // studentID set to totalCredit

  // others unchanged
  EXPECT_EQ((*table["s3"])[0], 823823); // original
  EXPECT_EQ((*table["s4"])[0], 66666);
  EXPECT_EQ((*table["s5"])[0], 777777);
  // prepare operands: SUB ( class class ) - subtract class from class, result 0
  std::vector<std::string> operands2 = {"class", "class"};

  std::vector<QueryCondition> conds2{cond1, cond2};

  auto qq = std::make_unique<SubQuery>("student_like", operands2, conds2);
  r = qq->execute();
  EXPECT_TRUE(r->success());
  std::ostringstream oss2;
  oss2 << *r;
  EXPECT_EQ(oss.str(), "Affected 2 rows.\n"); // s1 and s2 match

  EXPECT_EQ((*table["s1"])[1], 2014);
  EXPECT_EQ((*table["s2"])[1], 2014);

  // others unchanged
  EXPECT_EQ((*table["s3"])[1], 2015); // original
  EXPECT_EQ((*table["s4"])[1], 2015);
  EXPECT_EQ((*table["s5"])[1], 2016);

  // clean up
  Database::getInstance().dropTable("student_like");
}
