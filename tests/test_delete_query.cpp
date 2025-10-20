#include <gtest/gtest.h>
#include "../src/db/Table.h"
#include "../src/db/Database.h"
#include "../src/query/data/DeleteQuery.h"
#include "../src/query/Query.h"

// Helper to create a table with given rows and register it
static Table &createSimpleTable(const std::string &tableName)
{
    using TableV = Table::ValueType;
    auto t = std::make_unique<Table>(tableName, std::vector<std::string>{"age", "score"});
    Table &tableRef = Database::getInstance().registerTable(std::move(t));
    // insert some rows
    tableRef.insertByIndex("k1", std::vector<TableV>{20, 100});
    tableRef.insertByIndex("k2", std::vector<TableV>{19, 80});
    tableRef.insertByIndex("k3", std::vector<TableV>{21, 90});
    return tableRef;
}

TEST(DeleteQueryTest, DeleteByKeyExists)
{
    auto &table = createSimpleTable("del_by_key_exists");
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
    auto &table = createSimpleTable("del_by_field");
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
    auto &table = createSimpleTable("del_key_not_found");
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
    Table &table = Database::getInstance().registerTable(std::move(t));
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
    Table &table = Database::getInstance().registerTable(std::move(t));
    for (int i = 0; i < 5; ++i)
    {
        table.insertByIndex("k" + std::to_string(i), std::vector<TableV>{20 + i, 100 + i});
    }
    // delete where age >= 20 -> deletes all
    QueryCondition cond;
    cond.field = "age";
    cond.op = ">=";
    cond.value = "20";
    auto q = std::make_unique<DeleteQuery>("del_all", std::vector<std::string>{}, std::vector<QueryCondition>{cond});
    auto res = q->execute();
    EXPECT_TRUE(res->success());
    EXPECT_EQ(table.size(), 0u);
}

