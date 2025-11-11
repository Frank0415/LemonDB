#ifndef SRC_QEURY_EXCEPTION_H
#define SRC_QEURY_EXCEPTION_H

#include <stdexcept>
#include <string>

#include "formatter.h"

struct UnableToOpenFile : public std::runtime_error
{
  /**
   * Constructor for file open failure exception
   * @param file The name of the file that couldn't be opened
   */
  explicit UnableToOpenFile(const std::string& file)
      : std::runtime_error("Unable to open \"" + file + "\"")
  {
  }
};

struct DuplicatedTableName : public std::invalid_argument
{
  /**
   * Constructor for duplicated table name exception
   * @param str Error message describing the duplication
   */
  explicit DuplicatedTableName(const std::string& str) : std::invalid_argument(str)
  {
  }
};

struct TableNameNotFound : public std::invalid_argument
{
  /**
   * Constructor for table name not found exception
   * @param str Error message describing the missing table
   */
  explicit TableNameNotFound(const std::string& str) : std::invalid_argument(str)
  {
  }
};

struct ConflictingKey : public std::invalid_argument
{
  /**
   * Constructor for conflicting key exception
   * @param str Error message describing the key conflict
   */
  explicit ConflictingKey(const std::string& str) : std::invalid_argument(str)
  {
  }
};

struct NotFoundKey : public std::invalid_argument
{
  /**
   * Constructor for key not found exception
   * @param str Error message describing the missing key
   */
  explicit NotFoundKey(const std::string& str) : std::invalid_argument(str)
  {
  }
};

struct MultipleKey : public std::invalid_argument
{
  /**
   * Constructor for multiple key exception
   * @param str Error message describing multiple keys found
   */
  explicit MultipleKey(const std::string& str) : std::invalid_argument(str)
  {
  }
};

struct TableFieldNotFound : public std::out_of_range
{
  /**
   * Constructor for table field not found exception
   * @param str Error message describing the missing field
   */
  explicit TableFieldNotFound(const std::string& str) : std::out_of_range(str)
  {
  }
};

struct LoadFromStreamException : public std::runtime_error
{
  /**
   * Constructor for load from stream exception
   * @param str Error message describing the load failure
   */
  explicit LoadFromStreamException(const std::string& str) : std::runtime_error(str)
  {
  }
};

struct IllFormedQuery : public std::invalid_argument
{
  /**
   * Constructor for ill-formed query exception
   * @param str Error message describing the malformed query
   */
  explicit IllFormedQuery(const std::string& str) : std::invalid_argument(str)
  {
  }
};

struct IllFormedQueryCondition : public std::invalid_argument
{
  /**
   * Constructor for ill-formed query condition exception
   * @param str Error message describing the malformed condition
   */
  explicit IllFormedQueryCondition(const std::string& str) : std::invalid_argument(str)
  {
  }
};

class QueryBuilderMatchFailed : public std::invalid_argument
{
public:
  /**
   * Constructor for query builder match failed exception
   * @param qString The query string that failed to parse
   */
  explicit QueryBuilderMatchFailed(const std::string& qString)
      : std::invalid_argument(R"(Failed to parse query string: "?")"_f % qString)
  {
  }
};

#endif // SRC_QEURY_EXCEPTION_H
