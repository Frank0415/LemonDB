//
// Created by liu on 18-10-23.
//

#include "Table.h"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "../threading/Threadpool.h"
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
  return this->keyMap.find(key) != this->keyMap.end();
}

void Table::duplicateKeyData(const Table::KeyType& key)
{
  Table::KeyType copyKey(key);
  copyKey.append("_copy");
  std::vector<ValueType> copyData = ((*this)[key])->it->datumRef();
  this->insertByIndex(copyKey, std::move(copyData));
}

void Table::insertByIndex(const KeyType& key, std::vector<ValueType>&& data)
{
  if (this->keyMap.find(key) != this->keyMap.end())
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
  auto iterator = this->keyMap.find(key);

  // the key doesn't exist
  if (iterator == this->keyMap.end())
  {
    const std::string err =
        "In Table \"" + this->tableName + "\" : Key \"" + key + "\" doesn't exist!";
    throw NotFoundKey(err);
  }

  // the index of the key to delete
  const SizeType index = iterator->second;
  keyMap.erase(iterator);

  // swap the current data to the last one and pop back
  if (index != this->data.size() - 1)
  {
    Datum& lastDatum = this->data.back();
    // Save the key before moving
    const KeyType lastKey = lastDatum.keyConstRef();
    this->data[index] = std::move(lastDatum);
    this->keyMap[lastKey] = index;
  }
  data.pop_back();
}

Table::Object::Ptr Table::operator[](const Table::KeyType& key)
{
  auto iterator = this->keyMap.find(key);
  if (iterator == keyMap.end())
  {
    // not found
    return nullptr;
  }
  return createProxy(data.begin() +
                         static_cast<std::vector<Table::Datum>::difference_type>(iterator->second),
                     this);
}

std::ostream& operator<<(std::ostream& out, const Table& table)
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
    buffer << std::setw(width) << datum.keyConstRef();
    for (decltype(numFields) i = 0; i < numFields; ++i)
    {
      buffer << std::setw(width) << datum.datumConstRef()[i];
    }
    buffer << "\n";
  }
  return out << buffer.str();
}

void Table::addQuery(Query* query)
{
  queryQueueMutex.lock();
  if (query->isInstant() && !initialized && queryQueue.empty())
  {
    queryQueueMutex.unlock();
    query->execute();
    completeQuery();
    return;
  }
  if (queryQueueCounter < 0 || !initialized)
  {
    // writing, push back the query
    queryQueue.push_back(query);
    queryQueueMutex.unlock();
    return;
  }
  // idle/reading
  if (query->isWriter())
  {
    // add a writer or execute it if idle
    if (queryQueueCounter == 0 && queryQueue.empty())
    {
      queryQueueCounter = -1;
      queryQueueMutex.unlock();
      ThreadPool::getInstance().submit(
          [this, query]()
          {
            query->execute();
            completeQuery();
          });
    }
    else
    {
      queryQueue.push_back(query);
      queryQueueMutex.unlock();
    }
  }
  else
  {
    if (queryQueueCounter >= 0 && queryQueue.empty())
    {
      // add a reader and execute it at once if queue is empty
      ++queryQueueCounter;
      queryQueueMutex.unlock();
      ThreadPool::getInstance().submit(
          [this, query]()
          {
            query->execute();
            completeQuery();
          });
    }
    else
    {
      queryQueue.push_back(query);
      queryQueueMutex.unlock();
    }
  }
}

void Table::completeQuery()
{
  queryQueueMutex.lock();
  while (!queryQueue.empty() && !initialized && queryQueue.front()->isInstant())
  {
    auto* query = queryQueue.front();
    queryQueue.pop_front();
    queryQueueMutex.unlock();
    query->execute();
    queryQueueMutex.lock();
  }
  if (!initialized)
  {
    queryQueueMutex.unlock();
    return;
  }
  if (queryQueueCounter <= 0)
  {
    // writing or idle (should not happen), reset the counter
    queryQueueCounter = 0;
  }
  else
  {
    --queryQueueCounter;
  }
  if (queryQueue.empty())
  {
    queryQueueMutex.unlock();
    return;
  }
  if (queryQueueCounter == 0 && !queryQueue.empty() && queryQueue.front()->isWriter())
  {
    // if idle with a write query, execute it
    queryQueueCounter = -1;
    Query* query = queryQueue.front();
    queryQueue.pop_front();
    queryQueueMutex.unlock();
    ThreadPool::getInstance().submit(
        [this, query]()
        {
          query->execute();
          completeQuery();
        });
  }
  else
  {
    // if reading, execute all read query before next write query
    decltype(queryQueue) list;
    auto iter = std::find_if(queryQueue.begin(), queryQueue.end(),
                             [](const Query* query) { return query->isWriter(); });
    list.splice(list.begin(), queryQueue, queryQueue.begin(), iter);
    queryQueueCounter += static_cast<int>(list.size());
    queryQueueMutex.unlock();
    for (auto& item : list)
    {
      ThreadPool::getInstance().submit(
          [this, item]()
          {
            item->execute();
            completeQuery();
          });
    }
  }
}
