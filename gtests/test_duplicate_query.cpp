#include <gtest/gtest.h>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "../src/db/Database.h"
#include "../src/db/Table.h"
#include "../src/query/Query.h"
#include "../src/query/data/DuplicateQuery.h"

// A comprehensive test for DuplicateQuery that tests conditional copy_copy functionality
TEST(DuplicateQueryTest, DuplicateCreatesCopies)
{
  using TableV = Table::ValueType;
  // create table and register
  auto t = std::make_unique<Table>("dup_test", std::vector<std::string>{"f1", "f2"});
  Table& table = Database::getInstance().registerTable(std::move(t));

  // insert rows
  table.insertByIndex("k1", std::vector<TableV>{1, 10});
  table.insertByIndex("k2", std::vector<TableV>{2, 20});

  // prepare operands and condition: duplicate all rows (no WHERE)
  std::vector<std::string> operands; // none
  std::vector<QueryCondition> conds; // no WHERE

  auto q = std::make_unique<DuplicateQuery>("dup_test", operands, conds);
  auto r = q->execute();
  EXPECT_TRUE(r->success());
  std::ostringstream oss;
  oss << *r;

  // Expect two duplicates created
  EXPECT_EQ(table.size(), 4);
  // original keys should exist
  EXPECT_NE(table["k1"], nullptr);
  EXPECT_NE(table["k2"], nullptr);
  // duplicated keys should exist
  EXPECT_NE(table["k1_copy"], nullptr);
  EXPECT_NE(table["k2_copy"], nullptr);

  // verify data copied
  auto o1 = table["k1"];
  auto c1 = table["k1_copy"];
  EXPECT_EQ((*o1)[0], (*c1)[0]);
  EXPECT_EQ((*o1)[1], (*c1)[1]);

  QueryCondition cond;
  cond.field = "f2";
  cond.op = ">";
  cond.value = "15";
  cond.fieldId = 1; // f2 is the second field
  cond.valueParsed = static_cast<TableV>(15);
  conds.push_back(cond);

  std::vector<std::string> operands2; // no operands
  q = std::make_unique<DuplicateQuery>("dup_test", operands2, conds);
  r = q->execute();
  EXPECT_TRUE(r->success());
  std::ostringstream oss2;
  oss2 << *r;

  // Expect two duplicates created
  // Expect only one duplicate created (only k2 has f2 > 15)
  EXPECT_EQ(table.size(), 5);
  // original keys should exist
  EXPECT_NE(table["k1_copy"], nullptr);
  EXPECT_NE(table["k2_copy"], nullptr);
  // duplicated keys should exist
  // duplicated keys: only k2_copy should exist
  EXPECT_EQ(table["k1_copy_copy"], nullptr);
  EXPECT_NE(table["k2_copy_copy"], nullptr);

  // verify data copied
  auto o2 = table["k2_copy"];
  auto c2 = table["k2_copy_copy"];
  EXPECT_EQ((*o2)[0], (*c2)[0]);
  EXPECT_EQ((*o2)[1], (*c2)[1]);

  // clean up
  Database::getInstance().dropTable("dup_test");
}
