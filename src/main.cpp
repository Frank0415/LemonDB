//
// Created by liu on 18-10-21.
//

#include <getopt.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>

#include "query/Query.h"
#include "query/QueryBuilders.h"
#include "query/QueryParser.h"
#include "query/QueryResult.h"

#ifdef __has_feature
#if __has_feature(memory_sanitizer)
#define LEMONDB_WITH_MSAN 1
#endif
#endif
#ifdef __SANITIZE_MEMORY__
#define LEMONDB_WITH_MSAN 1
#endif

namespace
{
struct
{
  std::string listen;
  std::int64_t threads = 0;
} parsedArgs;

void parseArgs(int argc, char* argv[])
{
  const option longOpts[] = {{"listen", required_argument, nullptr, 'l'},
                             {"threads", required_argument, nullptr, 't'},
                             {nullptr, no_argument, nullptr, 0}};
  const char* shortOpts = "l:t:";
  int opt = 0;
  int longIndex = 0;
  while ((opt = getopt_long(argc, argv, shortOpts, longOpts, &longIndex)) != -1)
  {
    if (opt == 'l')
    {
      parsedArgs.listen = optarg;
    }
    else if (opt == 't')
    {
      parsedArgs.threads = std::strtol(optarg, nullptr, 10);
    }
    else
    {
      std::cerr << "lemondb: warning: unknown argument " << longOpts[longIndex].name << '\n';
    }
  }
}

std::string extractQueryString(std::istream& is)
{
  std::string buf;
  while (true)
  {
    const int ch = is.get();
    if (ch == ';')
    {
      return buf;
    }
    if (ch == EOF)
    {
      throw std::ios_base::failure("End of input");
    }
    buf.push_back(static_cast<char>(ch));
  }
}
} // namespace

int main(int argc, char* argv[])
{
  // Assume only C++ style I/O is used in lemondb
#ifdef LEMONDB_WITH_MSAN
  // Keep stdio synchronized to avoid MemorySanitizer false positives from unsynchronized buffers
  std::ios_base::sync_with_stdio(true);
#else
  // Disable sync for better throughput when sanitizers are not involved
  std::ios_base::sync_with_stdio(false);
#endif

  parseArgs(argc, argv);

  std::ifstream fin;
  std::istream* input = &std::cin;
  if (!parsedArgs.listen.empty())
  {
    fin.open(parsedArgs.listen);
    if (!fin.is_open())
    {
      std::cerr << "lemondb: error: " << parsedArgs.listen << ": no such file or directory" << '\n';
      exit(-1);
    }
    input = &fin;
  }

#ifdef NDEBUG
  // In production mode, listen argument must be defined
  if (parsedArgs.listen.empty())
  {
    std::cerr << "lemondb: error: --listen argument not found, not allowed in "
                 "production mode"
              << '\n';
    exit(-1);
  }
#else
  // In debug mode, use stdin as input if no listen file is found
  if (parsedArgs.listen.empty())
  {
    std::cerr << "lemondb: warning: --listen argument not found, use stdin "
                 "instead in debug mode"
              << std::endl;
    input = &std::cin;
  }
#endif

  std::istream& is = *input;

  if (parsedArgs.threads < 0)
  {
    std::cerr << "lemondb: error: threads num can not be negative value " << parsedArgs.threads
              << '\n';
    exit(-1);
  }
  else if (parsedArgs.threads == 0)
  {
    // @TODO Auto detect the thread num
    std::cerr << "lemondb: info: auto detect thread num" << '\n';
  }
  else
  {
    std::cerr << "lemondb: info: running in " << parsedArgs.threads << " threads" << '\n';
  }

  QueryParser p;

  p.registerQueryBuilder(std::make_unique<QueryBuilder(Debug)>());
  p.registerQueryBuilder(std::make_unique<QueryBuilder(ManageTable)>());
  p.registerQueryBuilder(std::make_unique<QueryBuilder(Complex)>());

  size_t counter = 0;

  while (is)
  {
    try
    {
      // A very standard REPL
      // REPL: Read-Evaluate-Print-Loop
      const std::string queryStr = extractQueryString(is);
      Query::Ptr query = p.parseQuery(queryStr);
      QueryResult::Ptr result = query->execute();
      std::cout << ++counter << "\n";
      if (result->success())
      {
        if (result->display())
        {
          std::cout << *result;
        }
        else
        {
#ifndef NDEBUG
          std::cout.flush();
          std::cerr << *result;
#endif
        }
      }
      else
      {
        std::cout.flush();
        std::cerr << "QUERY FAILED:\n\t" << *result;
      }
    }
    catch (const std::ios_base::failure& e)
    {
      // End of input
      break;
    }
    catch (const std::exception& e)
    {
      std::cout.flush();
      std::cerr << e.what() << '\n';
    }
  }

  return 0;
}

#ifdef LEMONDB_WITH_MSAN
#undef LEMONDB_WITH_MSAN
#endif
