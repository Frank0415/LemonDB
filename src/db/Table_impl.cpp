#include "Table.h"

void Table::setName(const std::string& name)
{
  this->tableName = name;
}

const std::string& Table::name() const
{
  return this->tableName;
}

bool Table::empty() const
{
  return this->data.empty();
}

size_t Table::size() const
{
  return this->data.size();
}

const std::vector<Table::FieldNameType>& Table::field() const
{
  return this->fields;
}

size_t Table::clear()
{
  auto result = keyMap.size();
  data.clear();
  keyMap.clear();
  return result;
}
