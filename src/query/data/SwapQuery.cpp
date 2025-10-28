#include "SwapQuery.h"

#include <exception>
#include <memory>
#include <stdexcept>
#include <string>

#include "../../db/Database.h"
#include "../../utils/formatter.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"

QueryResult::Ptr SwapQuery::execute()
{
  using namespace std;
  if (this->operands.size() != 2)
  {
    return make_unique<ErrorMsgResult>(qname, this->targetTable.c_str(),
                                       "Invalid number of operands (? operands)."_f %
                                           operands.size());
  }

  try
  {
    Database& db = Database::getInstance();
    auto& table = db[this->targetTable];
    if (operands[0] == "KEY" || operands[1] == "KEY")
    {
      return make_unique<ErrorMsgResult>(qname, this->targetTable,
                                         "Ill-formed query: KEY cannot be swapped.");
    }
    const auto f1 = table.getFieldIndex(operands[0]);
    const auto f2 = table.getFieldIndex(operands[1]);

    Table::SizeType counter = 0;
    const bool handled = this->testKeyCondition(table,
                                                [&](bool ok, Table::Object::Ptr&& obj)
                                                {
                                                  if (!ok)
                                                  {
                                                    return;
                                                  }
                                                  if (obj)
                                                  {
                                                    auto tmp = (*obj)[f1];
                                                    (*obj)[f1] = (*obj)[f2];
                                                    (*obj)[f2] = tmp;
                                                    ++counter;
                                                  }
                                                });

    if (!handled)
    {
      for (auto it = table.begin(); it != table.end(); ++it)
      {
        if (this->evalCondition(*it))
        {
          auto tmp = (*it)[f1];
          (*it)[f1] = (*it)[f2];
          (*it)[f2] = tmp;
          ++counter;
        }
      }
    }
    return make_unique<RecordCountResult>(static_cast<int>(counter));
  }
  catch (const TableNameNotFound&)
  {
    return make_unique<ErrorMsgResult>(qname, this->targetTable, "No such table.");
  }
  catch (const IllFormedQueryCondition& e)
  {
    return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
  }
  catch (const invalid_argument& e)
  {
    return make_unique<ErrorMsgResult>(qname, this->targetTable, "Unknown error '?'"_f % e.what());
  }
  catch (const exception& e)
  {
    return make_unique<ErrorMsgResult>(qname, this->targetTable, "Unkonwn error '?'."_f % e.what());
  }
}
std::string SwapQuery::toString()
{
  return "QUERY = SWAP \"" + this->targetTable + "\"";
}
