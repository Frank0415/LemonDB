#include "SumQuery.h"

#include <algorithm>
#include <cstddef>
#include <exception>
#include <memory>
#include <string>
#include <vector>

#include "../../db/Database.h"
#include "../../db/Table.h"
#include "../../utils/formatter.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"
#include "../../query/QueryResult.h"
#include "../../query/data/SumQuery.h"

QueryResult::Ptr SumQuery::execute()
{
  using namespace std;
  Database& db = Database::getInstance();

  try
  {
    auto& table = db[this->targetTable];
    if (this->operands.empty())
    {
      return make_unique<ErrorMsgResult>("SUM", this->targetTable, "Invalid number of fields");
    }
    // Check if any operand is "KEY" using std::any_of
    if (std::any_of(this->operands.begin(), this->operands.end(),
                    [](const auto& f) { return f == "KEY"; }))
    {
      return make_unique<ErrorMsgResult>("SUM", this->targetTable, "KEY cannot be summed.");
    }
    vector<Table::FieldIndex> fids;
    fids.reserve(this->operands.size());
    for (const auto& f : this->operands)
    {
      fids.emplace_back(table.getFieldIndex(f));
    }

    vector<Table::ValueType> sums(fids.size(), 0);
    bool handled = this->testKeyCondition(table,
                                          [&](bool ok, Table::Object::Ptr&& obj)
                                          {
                                            if (!ok)
                                            {
                                              return;
                                            }
                                            if (obj)
                                            {
                                              for (size_t i = 0; i < fids.size(); ++i)
                                              {
                                                sums[i] += (*obj)[fids[i]];
                                              }
                                            }
                                          });
    if (!handled)
    {
      for (auto it = table.begin(); it != table.end(); ++it)
      {
        if (this->evalCondition(*it))
        {
          for (size_t i = 0; i < fids.size(); ++i)
          {
            sums[i] += (*it)[fids[i]];
          }
        }
      }
    }
    return make_unique<SuccessMsgResult>(sums);
  }
  catch (const TableNameNotFound&)
  {
    return make_unique<ErrorMsgResult>("SUM", this->targetTable, "No such table.");
  }
  catch (const TableFieldNotFound&)
  {
    return make_unique<ErrorMsgResult>("SUM", this->targetTable, "No such field.");
  }
  catch (const IllFormedQueryCondition& e)
  {
    return make_unique<ErrorMsgResult>("SUM", this->targetTable, e.what());
  }
  catch (const std::exception& e)
  {
    return make_unique<ErrorMsgResult>("SUM", this->targetTable, "Unknown error '?'"_f % e.what());
  }
}

std::string SumQuery::toString()
{
  return "QUERY = SUM \"" + this->targetTable + "\"";
}
