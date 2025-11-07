//
// Created by liu on 18-10-23.
//

#ifndef PROJECT_DB_H
#define PROJECT_DB_H

#include <iostream>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "Table.h"

class Database
{
private:
  /**
   * A unique pointer to the global database object
   */
  static std::unique_ptr<Database> instance;

  /**
   * The map of tableName -> table unique ptr
   */
  std::unordered_map<std::string, Table::Ptr> tables;

  /**
   * Mutex to protect the tables map
   */
  mutable std::shared_mutex tablesMutex;

  /**
   * Mutex to protect the fileTableNameMap
   */
  mutable std::shared_mutex fileTableNameMutex;

  /**
   * The map of fileName -> tableName
   */
  std::unordered_map<std::string, std::string> fileTableNameMap;

  /**
   * Flag to indicate if QUIT has been called
   */
  bool endInput = false;

  /**
   * The default constructor is made private for singleton instance
   */
  Database() = default;

public:
  void testDuplicate(const std::string& tableName);

  Table& registerTable(Table::Ptr&& table);

  void dropTable(const std::string& tableName);

  void printAllTable();

  [[nodiscard]] Table& operator[](const std::string& tableName);

  [[nodiscard]] const Table& operator[](const std::string& tableName) const;

  Database& operator=(const Database&) = delete;

  Database& operator=(Database&&) = delete;

  Database(const Database&) = delete;

  Database(Database&&) = delete;

  ~Database() = default;

  [[nodiscard]] static Database& getInstance();

  void updateFileTableName(const std::string& fileName, const std::string& tableName);

  [[nodiscard]] std::string getFileTableName(const std::string& fileName);

  /**
   * Load a table from an input stream (i.e., a file)
   * @param is
   * @param source
   * @return reference of loaded table
   */
  static Table& loadTableFromStream(std::istream& input_stream, const std::string& source = "");

  /**
   * Signal that QUIT has been called and no more queries should be read
   */
  void exit();

  /**
   * Check if QUIT has been called
   */
  [[nodiscard]] bool isEnd() const
  {
    return endInput;
  }
};

#endif // PROJECT_DB_H
