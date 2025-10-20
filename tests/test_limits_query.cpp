// Test cases for MAX and MIN queries

#include <gtest/gtest.h>
#include <sstream>

#include "../src/db/Table.h"
#include "../src/db/Database.h"
#include "../src/query/data/MaxQuery.h"
#include "../src/query/data/MinQuery.h"
#include "../src/query/Query.h"

TEST(LimitsQueryTest, MaxAndMinSameTableNoCondition)
{
    using TableV = Table::ValueType;
    // create table and register
    auto t = std::make_unique<Table>("limits_test", std::vector<std::string>{"totalCredit", "class"});
    Table &table = Database::getInstance().registerTable(std::move(t));

    // insert rows
    table.insertByIndex("s1", std::vector<TableV>{112, 2014});
    table.insertByIndex("s2", std::vector<TableV>{150, 2016});
    table.insertByIndex("s3", std::vector<TableV>{100, 2013});
    // add many more rows (s4..s53) that do not change min/max
    for (int i = 4; i <= 53; ++i)
    {
        std::string key = "s" + std::to_string(i);
        TableV credit = 120 + (i % 20); // between 120..139
        TableV cls = 2000 + (i % 20);   // between 2000..2019
        table.insertByIndex(key, std::vector<TableV>{credit, cls});
    }

    // prepare operands and empty condition
    std::vector<std::string> operands = {"totalCredit", "class"};
    std::vector<QueryCondition> conds; // no WHERE

    // MAX
    auto qmax = std::make_unique<MaxQuery>("limits_test", operands, conds);
    auto rmax = qmax->execute();
    EXPECT_TRUE(rmax->success());
    std::ostringstream oss;
    oss << *rmax;
    EXPECT_EQ(oss.str(), "ANSWER = ( 150 2019 )\n");

    // MIN
    auto qmin = std::make_unique<MinQuery>("limits_test", operands, conds);
    auto rmin = qmin->execute();
    EXPECT_TRUE(rmin->success());
    std::ostringstream oss2;
    oss2 << *rmin;
    EXPECT_EQ(oss2.str(), "ANSWER = ( 100 2000 )\n");

    // clean up
    Database::getInstance().dropTable("limits_test");
}
