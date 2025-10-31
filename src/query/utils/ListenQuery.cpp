#include "ListenQuery.h"

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../../db/Database.h"
#include "../../query/QueryResult.h"

// constexpr const char *ListenQuery::qname;

QueryResult::Ptr ListenQuery::execute()
{
  try
  {
    std::ifstream infile(this->fileName);
    if (!infile.is_open())
    {
      return std::make_unique<ErrorMsgResult>(qname, "Cannot open file '?'"_f % this->fileName);
    }
    //// TODO: work on this query file
    std::vector<std::string> listen_results;
    // std::string const pure_file_name =
    // this->fileName.substr(this->fileName.find_last_of('/') + 1);

    std::string line;
    while (std::getline(infile, line))
    {
      listen_results.push_back(line);
    }

    infile.close();
    // For now just return a ListenResult with the filename; Manager integration
    // is not available in this build. The lines read are discarded.
    (void)listen_results;
    return std::make_unique<ListenResult>(fileName);
  }
  catch (const std::exception& e)
  {
    return std::make_unique<ErrorMsgResult>(qname, e.what());
  }
}

std::string ListenQuery::toString()
{
  return "QUERY = Listen, FILE = \"" + this->fileName + "\"";
}
