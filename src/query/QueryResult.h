//
// Created by liu on 18-10-25.
//

#ifndef PROJECT_QUERYRESULT_H
#define PROJECT_QUERYRESULT_H

#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "../utils/formatter.h"

class QueryResult {
public:
  using Ptr = std::unique_ptr<QueryResult>;

  [[nodiscard]] virtual bool success() = 0;

  [[nodiscard]] virtual bool display() = 0;

  virtual ~QueryResult() = default;

  friend std::ostream &operator<<(std::ostream &out, const QueryResult &table);

protected:
  virtual std::ostream &output(std::ostream &out) const = 0;
  static std::string buildMessage(std::string &&msg);
  static std::string buildMessage(const std::vector<int> &results);
};

class FailedQueryResult : public QueryResult {
  std::string data;

public:
  bool success() override { return false; }

  bool display() override { return false; }
};

class SucceededQueryResult : public QueryResult {
public:
  bool success() override { return true; }
};

class NullQueryResult : public SucceededQueryResult {
public:
  bool display() override { return false; }

protected:
  std::ostream &output(std::ostream &out) const override { return out << "\n"; }
};

class ErrorMsgResult : public FailedQueryResult {
  std::string msg;

public:
  /**
   * Construct an error result with query name and message
   * @param qname The name of the query that failed
   * @param msg The error message
   */
  ErrorMsgResult(const char *qname, const std::string &msg)
      : msg(buildMessage(R"(Query "?" failed : ?)"_f % qname % msg)) {}

  /**
   * Construct an error result with query name, table, and message
   * @param qname The name of the query that failed
   * @param table The name of the table where the error occurred
   * @param msg The error message
   */
  ErrorMsgResult(const char *qname, const std::string &table,
                 const std::string &msg)
      : msg(buildMessage(R"(Query "?" failed in Table "?" : ?)"_f % qname %
                         table % msg)) {}

protected:
  std::ostream &output(std::ostream &out) const override {
    return out << msg << "\n";
  }
};

class SuccessMsgResult : public SucceededQueryResult {
  bool debug_ = true;
  std::string msg;

public:
  bool display() override { return debug_; }

  /**
   * Construct a success result with a number
   */
  explicit SuccessMsgResult(const int number, bool debug = true)
      : debug_(debug), msg(buildMessage(R"(ANSWER = "?".)"_f % number)) {}

  /**
   * Construct a success result with a vector of results
   */
  explicit SuccessMsgResult(const std::vector<int> &results, bool debug = true)
      : debug_(debug), msg(buildMessage(results)) {}

  /**
   * Construct a success result with query name
   */
  explicit SuccessMsgResult(const char *qname, bool debug = false)
      : debug_(debug), msg(buildMessage(R"(Query "?" success.)"_f % qname)) {}

  /**
   * Construct a success result with query name and message
   */
  SuccessMsgResult(const char *qname, const std::string &msg,
                   bool debug = false)
      : debug_(debug),
        msg(buildMessage(R"(Query "?" success : ?)"_f % qname % msg)) {}

  /**
   * Construct a success result with query name, table, and message
   */
  SuccessMsgResult(const char *qname, const std::string &table,
                   const std::string &msg, bool debug = false)
      : debug_(debug),
        msg(buildMessage(R"(Query "?" success in Table "?" : ?)"_f % qname %
                         table % msg)) {}

protected:
  std::ostream &output(std::ostream &out) const override {
    return out << msg << "\n";
  }
};

class RecordCountResult : public SucceededQueryResult {
  int affectedRows;

public:
  bool display() override { return true; }

  /**
   * Construct a result showing the number of affected rows
   */
  explicit RecordCountResult(int count) : affectedRows(count) {}

protected:
  std::ostream &output(std::ostream &out) const override {
    return out << "Affected ? rows."_f % affectedRows << "\n";
  }
};

class TextRowsResult : public SucceededQueryResult {
  std::string payload;

public:
  bool display() override { return true; }

  /**
   * Construct a result with text payload
   */
  explicit TextRowsResult(std::string payload_str)
      : payload(std::move(payload_str)) {}

protected:
  std::ostream &output(std::ostream &out) const override {
    return out << payload;
  }
};

class ListenResult : public SucceededQueryResult {
  std::string listen_name;

public:
  bool display() override { return true; }

  /**
   * Construct a result for LISTEN query with the file name
   */
  explicit ListenResult(std::string name) : listen_name(std::move(name)) {}

protected:
  std::ostream &output(std::ostream &ostring) const override {
    ostring << "ANSWER = ( listening from " << listen_name << " )\n";
    return ostring;
  }
};

#endif  // PROJECT_QUERYRESULT_H
