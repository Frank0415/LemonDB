#include <deque>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <shared_mutex>

#include "../utils/formatter.h"
#include "../utils/uexception.h"
#include "Database.h"
#include "Table.h"

std::unique_ptr<Database> Database::instance = nullptr;

void Database::testDuplicate(const std::string& tableName)
{
  // NOTE: This method assumes the caller already holds tablesMutex
  auto iterator = this->tables.find(tableName);
  if (iterator != this->tables.end()) [[unlikely]]
  {
    throw DuplicatedTableName("Error when inserting table \"" + tableName +
                              "\". Name already exists.");
  }
}

Table& Database::registerTable(Table::Ptr&& table)
{
  const std::unique_lock lock(tablesMutex);
  auto name = table->name();
  this->testDuplicate(table->name());
  auto result = this->tables.emplace(name, std::move(table));
  return *(result.first->second);
}

Table& Database::operator[](const std::string& tableName)
{
  const std::shared_lock lock(tablesMutex);
  auto iterator = this->tables.find(tableName);
  if (iterator == this->tables.end()) [[unlikely]]
  {
    throw TableNameNotFound("Error accesing table \"" + tableName + "\". Table not found.");
  }
  return *(iterator->second);
}

const Table& Database::operator[](const std::string& tableName) const
{
  const std::shared_lock lock(tablesMutex);
  auto iterator = this->tables.find(tableName);
  if (iterator == this->tables.end()) [[unlikely]]
  {
    throw TableNameNotFound("Error accesing table \"" + tableName + "\". Table not found.");
  }
  return *(iterator->second);
}

void Database::dropTable(const std::string& tableName)
{
  const std::unique_lock lock(tablesMutex);
  auto iterator = this->tables.find(tableName);
  if (iterator == this->tables.end()) [[unlikely]]
  {
    throw TableNameNotFound("Error when trying to drop table \"" + tableName +
                            "\". Table not found.");
  }
  this->tables.erase(iterator);
}

void Database::printAllTable()
{
  const std::shared_lock lock(tablesMutex);
  const int width = 15;
  std::cout << "Database overview:" << '\n';
  std::cout << "=========================" << '\n';
  std::cout << std::setw(width) << "Table name";
  std::cout << std::setw(width) << "# of fields";
  std::cout << std::setw(width) << "# of entries" << '\n';
  for (const auto& table : this->tables) [[likely]]
  {
    std::cout << std::setw(width) << table.first;
    std::cout << std::setw(width) << (*table.second).field().size() + 1;
    std::cout << std::setw(width) << (*table.second).size() << '\n';
  }
  std::cout << "Total " << this->tables.size() << " tables." << '\n';
  std::cout << "=========================" << '\n';
}

Database& Database::getInstance()
{
  if (Database::instance == nullptr) [[unlikely]]
  {
    instance = std::unique_ptr<Database>(new Database);
  }
  return *instance;
}

void Database::updateFileTableName(const std::string& fileName, const std::string& tableName)
{
  const std::unique_lock lock(fileTableNameMutex);
  fileTableNameMap[fileName] = tableName;
}

std::string Database::getFileTableName(const std::string& fileName)
{
  const std::unique_lock lock(fileTableNameMutex);
  auto iterator = fileTableNameMap.find(fileName);
  if (iterator == fileTableNameMap.end()) [[unlikely]]
  {
    std::ifstream infile(fileName);
    if (!infile.is_open()) [[unlikely]]
    {
      return "";
    }
    std::string tableName;
    infile >> tableName;
    infile.close();
    fileTableNameMap.emplace(fileName, tableName);
    return tableName;
  }
  return iterator->second;
}

Table& Database::loadTableFromStream(std::istream& input_stream, const std::string& source)
{
  auto& database = Database::getInstance();
  const std::string errString = !source.empty() ? R"(Invalid table (from "?") format: )"_f % source
                                                : "Invalid table format: ";

  std::string tableName;
  Table::SizeType fieldCount = 0;
  std::deque<Table::KeyType> fields;

  std::string line;
  std::stringstream sstream;
  if (!std::getline(input_stream, line)) [[unlikely]]
  {
    throw LoadFromStreamException(errString + "Failed to read table metadata line.");
  }

  sstream.str(line);
  sstream >> tableName >> fieldCount;
  if (!sstream) [[unlikely]]
  {
    throw LoadFromStreamException(errString + "Failed to parse table metadata.");
  }

  std::unique_lock lock(database.tablesMutex);
  database.testDuplicate(tableName);
  lock.unlock(); // Explicitly unlock before potentially expensive file I/O

  if (!(std::getline(input_stream, line))) [[unlikely]]
  {
    throw LoadFromStreamException(errString + "Failed to load field names.");
  }

  sstream.clear();
  sstream.str(line);
  for (Table::SizeType i = 0; i < fieldCount; ++i) [[likely]]
  {
    std::string field;
    if (!(sstream >> field)) [[unlikely]]
    {
      throw LoadFromStreamException(errString + "Failed to load field names.");
    }
    fields.emplace_back(std::move(field));
  }

  if (fields.front() != "KEY") [[unlikely]]
  {
    throw LoadFromStreamException(errString + "Missing or invalid KEY field.");
  }

  fields.erase(fields.begin()); // Remove leading key
  auto table = std::make_unique<Table>(tableName, fields);

  // Pre-read all data lines to determine table size for pre-allocation
  std::vector<std::string> dataLines;
  Table::SizeType lineCount = 2;
  while (std::getline(input_stream, line)) [[likely]]
  {
    if (line.empty()) [[unlikely]]
    {
      break; // Read to an empty line
    }
    lineCount++;
    dataLines.emplace_back(std::move(line));
  }

  // Pre-allocate space for all rows to reduce allocation overhead
  table->reserve(dataLines.size());

  // Parse all data rows first
  std::vector<std::pair<Table::KeyType, std::vector<Table::ValueType>>> batchData;
  batchData.reserve(dataLines.size());

  for (Table::SizeType i = 0; i < dataLines.size(); ++i) [[likely]]
  {
    sstream.clear();
    sstream.str(dataLines[i]);
    std::string key;
    if (!(sstream >> key)) [[unlikely]]
    {
      throw LoadFromStreamException(errString + "Missing or invalid KEY field.");
    }
    std::vector<Table::ValueType> tuple;
    tuple.reserve(fieldCount - 1);
    for (Table::SizeType j = 1; j < fieldCount; ++j) [[likely]]
    {
      Table::ValueType value = 0;
      if (!(sstream >> value)) [[unlikely]]
      {
        throw LoadFromStreamException(errString + "Invalid row on LINE " +
                                      std::to_string(lineCount - dataLines.size() + i + 1));
      }
      tuple.emplace_back(value);
    }
    batchData.emplace_back(std::move(key), std::move(tuple));
  }

  // Batch insert all rows at once (with duplicate checking)
  table->insertBatch(std::move(batchData));

  return database.registerTable(std::move(table));
}

void Database::exit()
{
  // Set the flag to stop reading new queries
  endInput = true;
  // Note: Don't call std::exit(0) here!
  // Let main() handle waiting for queries and output
}
