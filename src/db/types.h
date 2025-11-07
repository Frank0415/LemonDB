#ifndef DB_TYPES_H
#define DB_TYPES_H

#include <memory>

class Table;

using TablePtr = std::unique_ptr<Table>;

using ValueType = int;

#endif // DB_TYPES_H