#include <gtest/gtest.h>
#include <sstream>

#include "../src/db/Database.h"
#include "../src/db/Table.h"
#include "../src/query/Query.h"
#include "../src/query/data/AddQuery.h"
#include "../src/query/data/SubQuery.h"

TEST(AddQueryTest, AddSumsFieldsIntoDestination) {
  using TableV = Table::ValueType;
  // create table and register
  auto t = std::make_unique<Table>("add_test",
                                   std::vector<std::string>{"f1", "f2", "sum"});
  Table &table = Database::getInstance().registerTable(std::move(t));

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

TEST(AddQueryTest, AddWithCondition) {
  using TableV = Table::ValueType;
  // create table and register
  auto t = std::make_unique<Table>("add_cond_test",
                                   std::vector<std::string>{"f1", "f2", "sum"});
  Table &table = Database::getInstance().registerTable(std::move(t));

  // insert rows
  table.insertByIndex("k1", std::vector<TableV>{1, 2, 0});
  table.insertByIndex("k2", std::vector<TableV>{3, 4, 0});
  table.insertByIndex("k3", std::vector<TableV>{5, 6, 0});

  // prepare operands and condition: ADD ( f1 f2 sum ) WHERE ( f1 > 2 )
  std::vector<std::string> operands = {"f1", "f2", "sum"};
  QueryCondition cond;
  cond.field = "f1";
  cond.op = ">";
  cond.value = "2";
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

TEST(SubQueryTest, DelSubFieldsIntoDestination) {
  using TableV = Table::ValueType;
  // create table and register
  auto t = std::make_unique<Table>(
      "sub_test", std::vector<std::string>{"f1", "f2", "diff"});
  Table &table = Database::getInstance().registerTable(std::move(t));

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

TEST(SubQueryTest, SubWithCondition) {
  using TableV = Table::ValueType;
  // create table and register
  auto t = std::make_unique<Table>(
      "sub_cond_test", std::vector<std::string>{"f1", "f2", "diff"});
  Table &table = Database::getInstance().registerTable(std::move(t));

  // insert rows
  table.insertByIndex("k1", std::vector<TableV>{5, 2, 0});
  table.insertByIndex("k2", std::vector<TableV>{10, 4, 0});
  table.insertByIndex("k3", std::vector<TableV>{15, 6, 0});

  // prepare operands and condition: SUB ( f1 f2 diff ) WHERE ( f1 >= 10 )
  std::vector<std::string> operands = {"f1", "f2", "diff"};
  QueryCondition cond;
  cond.field = "f1";
  cond.op = ">=";
  cond.value = "10";
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