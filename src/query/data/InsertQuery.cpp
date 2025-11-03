//
// Created by liu on 18-10-25.
//

#include "InsertQuery.h"

#include <cstdlib>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "../../db/Database.h"
#include "../../db/Table.h"
#include "../../db/TableLockManager.h"
#include "../../utils/formatter.h"
#include "../../utils/uexception.h"
#include "../QueryResult.h"

QueryResult::Ptr InsertQuery::execute()
{
  const int DECIMAL_BASE = 10;
  using std::string_literals::operator""s;
  if (this->getOperands().empty())
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef().c_str(),
                                            "No operand (? operands)."_f % getOperands().size());
  }
  Database& database = Database::getInstance();
  try
  {
    auto lock = TableLockManager::getInstance().acquireWrite(this->targetTableRef());
    auto& table = database[this->targetTableRef()];
    const auto& key = this->getOperands().front();
    std::vector<Table::ValueType> data;
    data.reserve(this->getOperands().size() - 1);
    for (auto it = ++this->getOperands().begin(); it != this->getOperands().end(); ++it)
    {
      data.emplace_back(strtol(it->c_str(), nullptr, DECIMAL_BASE));
    }
    table.insertByIndex(key, std::move(data));
    return std::make_unique<SuccessMsgResult>(qname, this->targetTableRef());
  }
  catch (const TableNameNotFound& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(), "No such table."s);
  }
  catch (const IllFormedQueryCondition& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(), e.what());
  }
  catch (const std::invalid_argument& e)
  {
    // Cannot convert operand to string
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            "Unknown error '?'"_f % e.what());
  }
  catch (const std::exception& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, this->targetTableRef(),
                                            "Unkonwn error '?'."_f % e.what());
  }
}

std::string InsertQuery::toString()
{
  return "QUERY = INSERT " + this->targetTableRef() + "\"";
}
