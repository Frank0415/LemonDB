#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../src/db/Database.h"
#include "../src/db/Table.h"
#include "../src/query/Query.h"
#include "../src/query/data/DeleteQuery.h"

// Helper to create a table with given rows and register it
static Table& createSimpleTable(const std::string& tableName)
{
  using TableV = Table::ValueType;
  auto t = std::make_unique<Table>(tableName, std::vector<std::string>{"age", "score"});
  Table& tableRef = Database::getInstance().registerTable(std::move(t));
  // insert some rows
  tableRef.insertByIndex("k1", std::vector<TableV>{20, 100});
  tableRef.insertByIndex("k2", std::vector<TableV>{19, 80});
  tableRef.insertByIndex("k3", std::vector<TableV>{21, 90});
  return tableRef;
}

TEST(DeleteQueryTest, DeleteByKeyExists)
{
  auto& table = createSimpleTable("del_by_key_exists");
  // build condition: (KEY = k2)
  QueryCondition cond;
  cond.field = "KEY";
  cond.op = "=";
  cond.value = "k2";
  std::vector<QueryCondition> conds{cond};
  // operands vector unused for DELETE in current implementation
  auto q = std::make_unique<DeleteQuery>("del_by_key_exists", std::vector<std::string>{}, conds);
  auto res = q->execute();
  EXPECT_TRUE(res->success());
  // table should have one less row and k2 should be missing
  EXPECT_EQ(table.size(), 2u);
  EXPECT_EQ(nullptr, table["k2"]);
}

TEST(DeleteQueryTest, DeleteByFieldCondition)
{
  auto& table = createSimpleTable("del_by_field");
  // delete where (age < 20) => should delete k2
  QueryCondition cond;
  cond.field = "age";
  cond.op = "<";
  cond.value = "20";
  std::vector<QueryCondition> conds{cond};
  auto q = std::make_unique<DeleteQuery>("del_by_field", std::vector<std::string>{}, conds);
  auto res = q->execute();
  EXPECT_TRUE(res->success());
  // only k2 had age 19
  EXPECT_EQ(table.size(), 2u);
  EXPECT_EQ(nullptr, table["k2"]);
}

TEST(DeleteQueryTest, DeleteByKeyNotFound)
{
  auto& table = createSimpleTable("del_key_not_found");
  // delete where KEY = not_exist
  QueryCondition cond;
  cond.field = "KEY";
  cond.op = "=";
  cond.value = "nope";
  std::vector<QueryCondition> conds{cond};
  auto q = std::make_unique<DeleteQuery>("del_key_not_found", std::vector<std::string>{}, conds);
  auto res = q->execute();
  // When no record matches the KEY condition, DeleteQuery returns success
  // with 0 affected rows (table unchanged)
  EXPECT_TRUE(res->success());
  EXPECT_EQ(table.size(), 3u);
}

TEST(DeleteQueryTest, DeleteAdjacentRows)
{
  // Create a table where two adjacent rows should be deleted
  using TableV = Table::ValueType;
  auto t = std::make_unique<Table>("del_adjacent", std::vector<std::string>{"age", "score"});
  Table& table = Database::getInstance().registerTable(std::move(t));
  table.insertByIndex("a1", std::vector<TableV>{10, 5});
  table.insertByIndex("a2", std::vector<TableV>{11, 6});
  table.insertByIndex("a3", std::vector<TableV>{12, 7});
  table.insertByIndex("a4", std::vector<TableV>{13, 8});

  // delete where age >= 11 and age <= 12 -> should remove a2 and a3 (adjacent)
  QueryCondition c1;
  c1.field = "age";
  c1.op = ">=";
  c1.value = "11";
  QueryCondition c2;
  c2.field = "age";
  c2.op = "<=";
  c2.value = "12";
  std::vector<QueryCondition> conds{c1, c2};
  auto q = std::make_unique<DeleteQuery>("del_adjacent", std::vector<std::string>{}, conds);
  auto res = q->execute();
  EXPECT_TRUE(res->success());
  // only a1 and a4 remain
  EXPECT_EQ(table.size(), 2u);
  EXPECT_NE(nullptr, table["a1"]);
  EXPECT_EQ(nullptr, table["a2"]);
  EXPECT_EQ(nullptr, table["a3"]);
  EXPECT_NE(nullptr, table["a4"]);
}

TEST(DeleteQueryTest, DeleteAllRows)
{
  // Create a table and delete all rows with a broad condition
  using TableV = Table::ValueType;
  auto t = std::make_unique<Table>("del_all", std::vector<std::string>{"age", "score"});
  Table& table = Database::getInstance().registerTable(std::move(t));
  for (int i = 0; i < 5; ++i)
  {
    table.insertByIndex("k" + std::to_string(i), std::vector<TableV>{20 + i, 100 + i});
  }
  // delete where age >= 20 -> deletes all
  QueryCondition cond;
  cond.field = "age";
  cond.op = ">=";
  cond.value = "20";
  auto q = std::make_unique<DeleteQuery>("del_all", std::vector<std::string>{},
                                         std::vector<QueryCondition>{cond});
  auto res = q->execute();
  EXPECT_TRUE(res->success());
  EXPECT_EQ(table.size(), 0u);
}

TEST(DeleteQueryTest, DeleteNonContiguousAndMultipleConditions)
{
  // Complex scenario: multiple conditions and non-contiguous deletions
  using TableV = Table::ValueType;
  auto t = std::make_unique<Table>("del_complex", std::vector<std::string>{"age", "score"});
  Table& table = Database::getInstance().registerTable(std::move(t));
  // keys k0..k6 with varying ages and scores
  table.insertByIndex("k0", std::vector<TableV>{18, 10});
  table.insertByIndex("k1", std::vector<TableV>{19, 20});
  table.insertByIndex("k2", std::vector<TableV>{20, 30});
  table.insertByIndex("k3", std::vector<TableV>{21, 40});
  table.insertByIndex("k4", std::vector<TableV>{22, 50});
  table.insertByIndex("k5", std::vector<TableV>{23, 60});
  table.insertByIndex("k6", std::vector<TableV>{24, 70});

  // Delete where (age >= 20 AND score < 60) OR age < 19
  // Note: current parser/condition supports only ANDed conditions; to simulate
  // an OR we run two deletes sequentially: first delete age < 19, then delete
  // age >=20 AND score < 60.

  // first delete age < 19 -> removes k0
  QueryCondition cond1;
  cond1.field = "age";
  cond1.op = "<";
  cond1.value = "19";
  auto q1 = std::make_unique<DeleteQuery>("del_complex", std::vector<std::string>{},
                                          std::vector<QueryCondition>{cond1});
  auto r1 = q1->execute();
  EXPECT_TRUE(r1->success());
  EXPECT_EQ(table.size(), 6u);
  EXPECT_EQ(nullptr, table["k0"]);

  // then delete age >= 20 AND score < 60 -> should remove k2 (score30), k3(score40), k4(score50)
  QueryCondition cA;
  cA.field = "age";
  cA.op = ">=";
  cA.value = "20";
  QueryCondition cB;
  cB.field = "score";
  cB.op = "<";
  cB.value = "60";
  auto q2 = std::make_unique<DeleteQuery>("del_complex", std::vector<std::string>{},
                                          std::vector<QueryCondition>{cA, cB});
  auto r2 = q2->execute();
  EXPECT_TRUE(r2->success());
  // removed k2,k3,k4 => remaining should be k1,k5,k6
  EXPECT_EQ(table.size(), 3u);
  EXPECT_EQ(nullptr, table["k2"]);
  EXPECT_EQ(nullptr, table["k3"]);
  EXPECT_EQ(nullptr, table["k4"]);
  EXPECT_NE(nullptr, table["k1"]);
  EXPECT_NE(nullptr, table["k5"]);
  EXPECT_NE(nullptr, table["k6"]);
}
