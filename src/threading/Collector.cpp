#include "Collector.h"
#include <sstream>
#include "db/Table.h"
#include <cstddef>

void executeQueryAsync(Query::Ptr query, size_t query_id, QueryResultCollector& g_result_collector)
{
  try
  {
    QueryResult::Ptr result = query->execute();

    std::ostringstream oss;
    if (result&& result->display())
    {
      oss << *result;
    }
    g_result_collector.addResult(query_id, oss.str());
  }
  catch (const std::exception& e)
  {
    std::ostringstream oss;
    oss << "Error: " << e.what() << "\n";
    g_result_collector.addResult(query_id, oss.str());
  }
}