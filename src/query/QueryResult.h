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

class QueryResult
{
public:
  using Ptr = std::unique_ptr<QueryResult>;

  [[nodiscard]] virtual bool success() = 0;

  [[nodiscard]] virtual bool display() = 0;

  virtual ~QueryResult() = default;

  friend std::ostream& operator<<(std::ostream& out, const QueryResult& table);

protected:
  virtual std::ostream& output(std::ostream& out) const = 0;
};

class FailedQueryResult : public QueryResult
{
  std::string data;

public:
  bool success() override
  {
    return false;
  }

  bool display() override
  {
    return false;
  }
};

class SucceededQueryResult : public QueryResult
{
public:
  bool success() override
  {
    return true;
  }
};

class NullQueryResult : public SucceededQueryResult
{
public:
  bool display() override
  {
    return false;
  }

protected:
  std::ostream& output(std::ostream& out) const override
  {
    return out << "\n";
  }
};

class ErrorMsgResult : public FailedQueryResult
{
  std::string msg;

public:
  ErrorMsgResult(const char* qname, const std::string& msg)
  {
    this->msg = R"(Query "?" failed : ?)"_f % qname % msg;
  }

  ErrorMsgResult(const char* qname, const std::string& table, const std::string& msg)
  {
    this->msg = R"(Query "?" failed in Table "?" : ?)"_f % qname % table % msg;
  }

protected:
  std::ostream& output(std::ostream& out) const override
  {
    return out << msg << "\n";
  }
};

class SuccessMsgResult : public SucceededQueryResult
{
  std::string msg;
  bool debug_ = true;

public:
  bool display() override
  {
    return debug_;
  }

  explicit SuccessMsgResult(const int number, bool debug = true) : debug_(debug)
  {
    this->msg = R"(ANSWER = "?".)"_f % number;
  }

  explicit SuccessMsgResult(const std::vector<int>& results, bool debug = true) : debug_(debug)
  {
    std::stringstream stream;
    stream << "ANSWER = ( ";
    for (auto result : results)
    {
      stream << result << " ";
    }
    stream << ")";
    this->msg = stream.str();
  }

  explicit SuccessMsgResult(const char* qname, bool debug = false) : debug_(debug)
  {
    this->msg = R"(Query "?" success.)"_f % qname;
  }

  SuccessMsgResult(const char* qname, const std::string& msg, bool debug = false) : debug_(debug)
  {
    this->msg = R"(Query "?" success : ?)"_f % qname % msg;
  }

  SuccessMsgResult(const char* qname,
                   const std::string& table,
                   const std::string& msg,
                   bool debug = false)
      : debug_(debug)
  {
    this->msg = R"(Query "?" success in Table "?" : ?)"_f % qname % table % msg;
  }

protected:
  std::ostream& output(std::ostream& out) const override
  {
    return out << msg << "\n";
  }
};

class RecordCountResult : public SucceededQueryResult
{
  int affectedRows;

public:
  bool display() override
  {
    return true;
  }

  explicit RecordCountResult(int count) : affectedRows(count)
  {
  }

protected:
  std::ostream& output(std::ostream& out) const override
  {
    return out << "Affected ? rows."_f % affectedRows << "\n";
  }
};

class TextRowsResult : public SucceededQueryResult
{
  std::string payload;

public:
  bool display() override
  {
    return true;
  }

  explicit TextRowsResult(std::string payload_str) : payload(std::move(payload_str))
  {
  }

protected:
  std::ostream& output(std::ostream& out) const override
  {
    return out << payload;
  }
};

#endif // PROJECT_QUERYRESULT_H
