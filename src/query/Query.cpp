//
// Created by liu on 18-10-25.
//

#include "Query.h"

#include <cassert>
#include <cstdlib>
#include <functional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

#include "db/Table.h"
#include "utils/formatter.h"
#include "utils/uexception.h"

std::pair<std::string, bool> ComplexQuery::initCondition(const Table& table)
{
  const std::unordered_map<std::string, int> opmap{
      {">", '>'}, {"<", '<'}, {"=", '='}, {">=", 'g'}, {"<=", 'l'},
  };
  std::pair<std::string, bool> result = {"", true};
  for (auto& cond : condition) [[likely]]
  {
    if (cond.field == "KEY") [[unlikely]]
    {
      if (cond.op != "=") [[unlikely]]
      {
        throw IllFormedQueryCondition("Can only compare equivalence on KEY");
      }
      if (result.first.empty()) [[likely]]
      {
        result.first = cond.value;
      }
      else if (result.first != cond.value) [[unlikely]]
      {
        result.second = false;
        return result;
      }
    }
    else [[likely]]
    {
      constexpr int decimal_base = 10;
      cond.fieldId = table.getFieldIndex(cond.field);
      cond.valueParsed =
          static_cast<Table::ValueType>(std::strtol(cond.value.c_str(), nullptr, decimal_base));
      int operator_index = 0;
      try
      {
        operator_index = opmap.at(cond.op);
      }
      catch (const std::out_of_range& e)
      {
        throw IllFormedQueryCondition(R"("?" is not a valid condition operator.)"_f % cond.op);
      }
      switch (operator_index)
      {
      case '>':
        cond.comp = std::greater<>();
        break;
      case '<':
        cond.comp = std::less<>();
        break;
      case '=':
        cond.comp = std::equal_to<>();
        break;
      case 'g':
        cond.comp = std::greater_equal<>();
        break;
      case 'l':
        cond.comp = std::less_equal<>();
        break;
      default:
        assert(0); // should never be here
      }
    }
  }
  return result;
}

bool ComplexQuery::evalCondition(const Table::Object& object)
{
  bool ret = true;
  for (const auto& cond : condition) [[likely]]
  {
    if (cond.field != "KEY") [[likely]]
    {
      ret = ret && cond.comp(object[cond.fieldId], cond.valueParsed);
    }
    else [[unlikely]]
    {
      ret = ret && (object.key() == cond.value);
    }
  }
  return ret;
}

bool ComplexQuery::testKeyCondition(Table& table,
                                    const std::function<void(bool, Table::Object::Ptr&&)>& function)
{
  auto condResult = initCondition(table);
  if (!condResult.second) [[unlikely]]
  {
    function(false, nullptr);
    return true;
  }
  if (!condResult.first.empty()) [[unlikely]]
  {
    auto object = table[condResult.first];
    if (object != nullptr && evalCondition(*object)) [[likely]]
    {
      function(true, std::move(object));
    }
    else [[unlikely]]
    {
      function(false, nullptr);
    }
    return true;
  }
  return false;
}
