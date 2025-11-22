//
// Created by liu on 18-10-23.
//

#ifndef PROJECT_DB_TABLE_H
#define PROJECT_DB_TABLE_H

#include <cstddef>
#include <iterator>
#include <limits>
#include <list>
#include <memory>
#include <mutex>
#include <ostream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "Datum.h"
#include "QueryBase.h"
#include "utils/formatter.h"
#include "utils/uexception.h"

class Table {
public:
  using KeyType = std::string;
  using FieldNameType = std::string;
  using FieldIndex = size_t;
  using ValueType = int;
  static constexpr const ValueType ValueTypeMax =
      std::numeric_limits<ValueType>::max();
  static constexpr const ValueType ValueTypeMin =
      std::numeric_limits<ValueType>::min();
  using SizeType = size_t;

private:
  /** A row in the table */

  using DataIterator = std::vector<Datum>::iterator;
  using ConstDataIterator = std::vector<Datum>::const_iterator;

  /** The fields, ordered as defined in fieldMap */
  std::vector<FieldNameType> fields;
  /** Map field name into index */
  std::unordered_map<FieldNameType, FieldIndex> fieldMap;

  /** The rows are saved in a vector, which is unsorted */
  std::vector<Datum> data;
  /** Used to keep the keys unique and provide O(1) access with key */
  std::unordered_map<KeyType, SizeType> keyMap;

  /** The name of table */
  std::string tableName;

  bool initialized = false;
  std::list<Query *> queryQueue;
  int queryQueueCounter = 0;
  std::mutex queryQueueMutex;

public:
  static constexpr size_t splitsize() {
    constexpr size_t part = 2000;
    return part;
  }

  using Ptr = std::unique_ptr<Table>;

  /**
   * A proxy class that provides abstraction on internal Implementation.
   * Allows independent variation on the representation for a table object
   *
   * @tparam Iterator
   * @tparam VType
   */
  template <class Iterator, class VType> class ObjectImpl {
    friend class Table;

    /** Not const because key can be updated */
    Iterator it;
    // Use const Table* for const VType, Table* for non-const VType
    using TablePtrType =
        std::conditional_t<std::is_const_v<VType>, const Table *, Table *>;
    TablePtrType table;

  public:
    using Ptr = std::unique_ptr<ObjectImpl>;

    ObjectImpl(Iterator datumIt, TablePtrType table_ptr)
        : it(datumIt), table(table_ptr) {}

    ObjectImpl(const ObjectImpl &) = default;
    ObjectImpl(ObjectImpl &&) noexcept = default;
    ObjectImpl &operator=(const ObjectImpl &) = default;
    ObjectImpl &operator=(ObjectImpl &&) noexcept = default;
    ~ObjectImpl() = default;

    [[nodiscard]] const KeyType &key() const { return it->keyConstRef(); }

    // Helper to obtain the correct datum reference type depending on VType
    template <typename T = VType> [[nodiscard]] auto &datumAccess() const {
      if constexpr (std::is_const_v<T>) {
        return it->datumConstRef();
      } else {
        return it->datumRef();
      }
    }

    void setKey(KeyType key) {
      auto keyMapIt = table->keyMap.find(it->keyConstRef());
      auto dataIt = std::move(keyMapIt->second);
      table->keyMap.erase(keyMapIt);
      table->keyMap.emplace(key, std::move(dataIt));
      it->setKey(std::move(key));
    }

    /**
     * Accessing by index should be, at least as fast as accessing by field
     * name. Clients should prefer accessing by index if the same field is
     * accessed frequently (the implement is improved so that index is actually
     * faster now)
     */
    VType &operator[](const FieldNameType &field) const {
      try {
        auto &index = table->fieldMap.at(field);
        return datumAccess().at(index);
      } catch (const std::out_of_range &e) {
        throw TableFieldNotFound(R"(Field name "?" doesn't exists.)"_f %
                                 (field));
      }
    }

    VType &operator[](const FieldIndex &index) const {
      try {
        return datumAccess().at(index);
      } catch (const std::out_of_range &e) {
        throw TableFieldNotFound(R"(Field index ? out of range.)"_f % (index));
      }
    }

    [[nodiscard]] VType &get(const FieldNameType &field) const {
      try {
        auto &index = table->fieldMap.at(field);
        return datumAccess().at(index);
      } catch (const std::out_of_range &e) {
        throw TableFieldNotFound(R"(Field name "?" doesn't exists.)"_f %
                                 (field));
      }
    }

    [[nodiscard]] VType &get(const FieldIndex &index) const {
      try {
        return datumAccess().at(index);
      } catch (const std::out_of_range &e) {
        throw TableFieldNotFound(R"(Field index ? out of range.)"_f % (index));
      }
    }
  };

  using Object = ObjectImpl<DataIterator, ValueType>;
  using ConstObject = ObjectImpl<ConstDataIterator, const ValueType>;

  /**
   * A proxy class that provides iteration on the table
   * @tparam ObjType
          return datumAccess().at(index);
   */
  template <typename ObjType, typename DatumIterator> class IteratorImpl {
    using difference_type = std::ptrdiff_t;
    using value_type = ObjType;
    using pointer = typename ObjType::Ptr;
    using reference = ObjType;
    using iterator_category = std::random_access_iterator_tag;
    // See https://stackoverflow.com/questions/37031805/

    friend class Table;

    DatumIterator it;
    // Use const Table* for ConstObject, Table* for Object
    using TablePtrType = typename ObjType::TablePtrType;
    TablePtrType table = nullptr;

  public:
    IteratorImpl(DatumIterator datumIt, TablePtrType table_ptr)
        : it(datumIt), table(table_ptr) {}

    IteratorImpl() = default;

    IteratorImpl(const IteratorImpl &) = default;

    IteratorImpl(IteratorImpl &&) noexcept = default;

    IteratorImpl &operator=(const IteratorImpl &) = default;

    IteratorImpl &operator=(IteratorImpl &&) noexcept = default;

    ~IteratorImpl() = default;

    pointer operator->() { return createProxy(it, table); }

    reference operator*() { return *createProxy(it, table); }

    pointer operator->() const {
      if constexpr (std::is_same_v<ObjType, ConstObject>) {
        return createProxy(it, table);
      } else {
        return std::make_unique<Object>(it, table);
      }
    }

    reference operator*() const {
      if constexpr (std::is_same_v<ObjType, ConstObject>) {
        return *createProxy(it, table);
      } else {
        return *std::make_unique<Object>(it, table);
      }
    }

    IteratorImpl operator+(int n) { return IteratorImpl(it + n, table); }

    IteratorImpl operator-(int n) { return IteratorImpl(it - n, table); }

    IteratorImpl &operator+=(int n) { return it += n, *this; }

    IteratorImpl &operator-=(int n) { return it -= n, *this; }

    IteratorImpl &operator++() { return ++it, *this; }

    IteratorImpl &operator--() { return --it, *this; }

    IteratorImpl operator++(int) {
      auto retVal = IteratorImpl(*this);
      ++it;
      return retVal;
    }

    IteratorImpl operator--(int) {
      auto retVal = IteratorImpl(*this);
      --it;
      return retVal;
    }

    bool operator==(const IteratorImpl &other) const {
      return this->it == other.it;
    }

    friend bool operator!=(const IteratorImpl &lhs, const IteratorImpl &rhs) {
      return lhs.it != rhs.it;
    }

    bool operator<=(const IteratorImpl &other) { return this->it <= other.it; }

    bool operator>=(const IteratorImpl &other) { return this->it >= other.it; }

    bool operator<(const IteratorImpl &other) { return this->it < other.it; }

    bool operator>(const IteratorImpl &other) { return this->it > other.it; }
  };

  using Iterator = IteratorImpl<Object, decltype(data.begin())>;
  using ConstIterator = IteratorImpl<ConstObject, decltype(data.cbegin())>;

private:
  static ConstObject::Ptr createProxy(ConstDataIterator iterator,
                                      const Table *table) {
    return std::make_unique<ConstObject>(iterator, table);
  }

  static Object::Ptr createProxy(DataIterator iterator, Table *table) {
    return std::make_unique<Object>(iterator, table);
  }

public:
  Table() = delete;

  explicit Table(std::string name) : tableName(std::move(name)) {}

  /**
   * Accept any container that contains string.
   * @tparam FieldIDContainer
   * @param name: the table name (must be unique in the database)
   * @param fields: an iterable container with fields
   */
  template <class FieldIDContainer>
  Table(const std::string &name, const FieldIDContainer &fields);

  /**
   * Copy constructor from another table
   * @param name: the table name (must be unique in the database)
   * @param origin: the original table copied from
   */
  Table(std::string name, const Table &origin)
      : fields(origin.fields), fieldMap(origin.fieldMap), data(origin.data),
        keyMap(origin.keyMap), tableName(std::move(name)) {}

  /**
   * Check whether a key already exists in the table
   * @param key
   * @return
   */
  bool evalDuplicateCopy(KeyType key);

  /**
   * Duplicate a row of data by its key
   * @param key
   */
  void duplicateKeyData(const KeyType &key);

  /**
   * Find the index of a field in the fieldMap
   * @param field
   * @return fieldIndex
   */
  [[nodiscard]] FieldIndex getFieldIndex(const FieldNameType &field) const;

  /**
   * Insert a row of data by its key
   * @tparam ValueTypeContainer
   * @param key
   * @param data
   */
  void insertByIndex(const KeyType &key, std::vector<ValueType> &&data);

  /**
   * Insert multiple rows of data by their keys (batch operation)
   * Checks for key conflicts before inserting any data
   * @param batch: vector of key-data pairs to insert
   */
  void
  insertBatch(std::vector<std::pair<KeyType, std::vector<ValueType>>> &&batch);

  /**
   * Delete a row of data by its key
   * @param key
   */
  void deleteByIndex(const KeyType &key);

  /**
   * Access the value according to the key
   * @param key
   * @return the Object that KEY = key, or nullptr if key doesn't exist
   */
  Object::Ptr operator[](const KeyType &key);

  /**
   * Set the name of the table
   * @param name
   */
  void setName(const std::string &name);

  /**
   * Get the name of the table
   * @return
   */
  [[nodiscard]] const std::string &name() const;

  /**
   * Return whether the table is empty
   * @return
   */
  [[nodiscard]] bool empty() const;

  /**
   * Return the num of data stored in the table
   * @return
   */
  [[nodiscard]] size_t size() const;

  /**
   * Return the fields in the table
   * @return
   */
  [[nodiscard]] const std::vector<FieldNameType> &field() const;

  /**
   * Clear all content in the table
   * @return rows affected
   */
  size_t clear();

  /**
   * Pre-allocate capacity for bulk load operations
   * Reserves space for data vector and keyMap to reduce allocation overhead
   * @param capacity number of rows to pre-allocate
   */
  void reserve(SizeType capacity) {
    data.reserve(capacity);
    keyMap.reserve(capacity);
  }

  void drop();

  /**
   * Get a begin iterator similar to the standard iterator
   * @return begin iterator
   */
  Iterator begin();

  /**
   * Get a end iterator similar to the standard iterator
   * @return end iterator
   */
  Iterator end();

  /**
   * Get a const begin iterator similar to the standard iterator
   * @return const begin iterator
   */
  [[nodiscard]] ConstIterator begin() const;

  /**
   * Get a const end iterator similar to the standard iterator
   * @return const end iterator
   */
  [[nodiscard]] ConstIterator end() const;

  /**
   * Overload the << operator for complete print of the table
   * @param os
   * @param table
   * @return the origin ostream
   */
  friend std::ostream &operator<<(std::ostream &out, const Table &table);

  void addQuery(Query *query);
  void completeQuery();
  [[nodiscard]] bool isInited() const;
};

std::ostream &operator<<(std::ostream &out, const Table &table);

template <class FieldIDContainer>
Table::Table(const std::string &name, const FieldIDContainer &fields)
    : fields(fields.cbegin(), fields.cend()), tableName(name) {
  SizeType index = 0;
  for (const auto &fieldName : fields) {
    if (fieldName == "KEY") {
      throw MultipleKey("Error creating table \"" + name +
                        "\": Multiple KEY field.");
    }
    fieldMap.emplace(fieldName, index++);
  }
}

#endif  // PROJECT_DB_TABLE_H
