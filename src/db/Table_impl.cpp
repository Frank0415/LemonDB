#include <string>
#include <vector>
#include <cstddef>

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

void Table::drop()
{
  queryQueueCounter = 0;
  fields.clear();
  fieldMap.clear();
  data.clear();
  keyMap.clear();
  queryQueueMutex.lock();
  initialized = false;
  queryQueueMutex.unlock();
}

Table::Iterator Table::begin()
{
  return {data.begin(), this};
}

Table::Iterator Table::end()
{
  return {data.end(), this};
}

Table::ConstIterator Table::begin() const
{
  return {data.cbegin(), this};
}

Table::ConstIterator Table::end() const
{
  return {data.cend(), this};
}

bool Table::isInited() const
{
  return initialized;
}
