//
// Created by liu on 18-10-23.
//

#include "Table.h"

#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "../utils/formatter.h"
#include "../utils/uexception.h"

Table::FieldIndex Table::getFieldIndex(const Table::FieldNameType& field) const
{
  try
  {
    return this->fieldMap.at(field);
  }
  catch (const std::out_of_range& e)
  {
    throw TableFieldNotFound(R"(Field name "?" doesn't exists.)"_f % (field));
  }
}

bool Table::evalDuplicateCopy(Table::KeyType key)
{
  key = key.append("_copy");
  return this->keyMap.contains(key);
}

void Table::duplicateKeyData(const Table::KeyType& key)
{
  Table::KeyType copyKey(key);
  copyKey.append("_copy");
  std::vector<ValueType> copyData = ((*this)[key])->it->datum;
  this->insertByIndex(copyKey, std::move(copyData));
}

void Table::insertByIndex(const KeyType& key, std::vector<ValueType>&& data)
{
  if (this->keyMap.contains(key))
  {
    const std::string err =
        "In Table \"" + this->tableName + "\" : Key \"" + key + "\" already exists!";
    throw ConflictingKey(err);
  }
  this->keyMap.emplace(key, this->data.size());
  this->data.emplace_back(key, std::move(data));
}

void Table::deleteByIndex(const KeyType& key)
{
  // the key to delete
  auto it = this->keyMap.find(key);

  // the key doesn't exist
  if (it == this->keyMap.end())
  {
    const std::string err =
        "In Table \"" + this->tableName + "\" : Key \"" + key + "\" doesn't exist!";
    throw NotFoundKey(err);
  }

  // the index of the key to delete
  const SizeType index = it->second;
  keyMap.erase(it);

  // swap the current data to the last one and pop back
  if (index != this->data.size() - 1)
  {
    Datum& lastDatum = this->data.back();
    // Save the key before moving
    const KeyType lastKey = lastDatum.key;
    this->data[index] = std::move(lastDatum);
    this->keyMap[lastKey] = index;
  }
  data.pop_back();
}

Table::Object::Ptr Table::operator[](const Table::KeyType& key)
{
  auto it = keyMap.find(key);
  if (it == keyMap.end())
  {
    // not found
    return nullptr;
  }
  return createProxy(
      data.begin() + static_cast<std::vector<Table::Datum>::difference_type>(it->second), this);
}

std::ostream& operator<<(std::ostream& os, const Table& table)
{
  const int width = 10;
  std::stringstream buffer;
  buffer << table.tableName << "\t" << (table.fields.size() + 1) << "\n";
  buffer << std::setw(width) << "KEY";
  for (const auto& field : table.fields)
  {
    buffer << std::setw(width) << field;
  }
  buffer << "\n";
  auto numFields = table.fields.size();
  for (const auto& datum : table.data)
  {
    buffer << std::setw(width) << datum.key;
    for (decltype(numFields) i = 0; i < numFields; ++i)
    {
      buffer << std::setw(width) << datum.datum[i];
    }
    buffer << "\n";
  }
  return os << buffer.str();
}
