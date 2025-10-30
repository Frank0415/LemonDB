//
// Created by liu on 18-10-23.
//

#ifndef PROJECT_DB_TABLE_H
#define PROJECT_DB_TABLE_H

#include <cstddef>
#include <iterator>
#include <limits>
#include <memory>
#include <ostream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "../utils/formatter.h"
#include "../utils/uexception.h"

#define DBTABLE_ACCESS_WITH_NAME_EXCEPTION(field)                                                  \
  try                                                                                              \
  {                                                                                                \
    auto& index = table->fieldMap.at(field);                                                       \
    return it->datum.at(index);                                                                    \
  }                                                                                                \
  catch (const std::out_of_range& e)                                                               \
  {                                                                                                \
    throw TableFieldNotFound(R"(Field name "?" doesn't exists.)"_f % (field));                     \
  }

#define DBTABLE_ACCESS_WITH_INDEX_EXCEPTION(index)                                                 \
  try                                                                                              \
  {                                                                                                \
    return it->datum.at(index);                                                                    \
  }                                                                                                \
  catch (const std::out_of_range& e)                                                               \
  {                                                                                                \
    throw TableFieldNotFound(R"(Field index ? out of range.)"_f % (index));                        \
  }

class Table
{
public:
  using KeyType = std::string;
  using FieldNameType = std::string;
  using FieldIndex = size_t;
  using ValueType = int;
  static constexpr const ValueType ValueTypeMax = std::numeric_limits<ValueType>::max();
  static constexpr const ValueType ValueTypeMin = std::numeric_limits<ValueType>::min();
  using SizeType = size_t;

private:
  /** A row in the table */
  struct Datum
  {
    /** Unique key of this datum */
    KeyType key;
    /** The values in the order of fields */
    std::vector<ValueType> datum;

    Datum() = default;

    // By declaring all 5 special member functions, we adhere to the Rule of
    // Five.
    Datum(const Datum&) = default;
    Datum&
    operator=(const Datum&) = default; // Fix: Explicitly default the copy assignment operator.
    Datum(Datum&&) noexcept = default;
    Datum& operator=(Datum&&) noexcept = default;
    ~Datum() = default;

    explicit Datum(const SizeType& size) : datum(size, ValueType())
    {
    }

    template <class ValueTypeContainer>
    explicit Datum(const KeyType& key, const ValueTypeContainer& datum) : key(key), datum(datum)
    {
    }

    explicit Datum(const KeyType& key, std::vector<ValueType>&& datum) noexcept
        : key(key), datum(std::move(datum))
    {
    }
  };

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

public:
  using Ptr = std::unique_ptr<Table>;

  /**
   * A proxy class that provides abstraction on internal Implementation.
   * Allows independent variation on the representation for a table object
   *
   * @tparam Iterator
   * @tparam VType
   */
  template <class Iterator, class VType> class ObjectImpl
  {
    friend class Table;

    /** Not const because key can be updated */
    Iterator it;
    Table* table;

  public:
    using Ptr = std::unique_ptr<ObjectImpl>;

    ObjectImpl(Iterator datumIt, const Table* table_ptr)
        : it(datumIt), table(const_cast<Table*>(table_ptr))
    {
    }

    ObjectImpl(const ObjectImpl&) = default;

    ObjectImpl(ObjectImpl&&) noexcept = default;

    ObjectImpl& operator=(const ObjectImpl&) = default;

    ObjectImpl& operator=(ObjectImpl&&) noexcept = default;

    ~ObjectImpl() = default;

    [[nodiscard]] const KeyType& key() const
    {
      return it->key;
    }

    void setKey(KeyType key)
    {
      auto keyMapIt = table->keyMap.find(it->key);
      auto dataIt = std::move(keyMapIt->second);
      table->keyMap.erase(keyMapIt);
      table->keyMap.emplace(key, std::move(dataIt));
      it->key = std::move(key);
    }

    /**
     * Accessing by index should be, at least as fast as accessing by field
     * name. Clients should prefer accessing by index if the same field is
     * accessed frequently (the implement is improved so that index is actually
     * faster now)
     */
    VType& operator[](const FieldNameType& field) const
    {
      DBTABLE_ACCESS_WITH_NAME_EXCEPTION(field);
    }

    VType& operator[](const FieldIndex& index) const
    {
      DBTABLE_ACCESS_WITH_INDEX_EXCEPTION(index);
    }

    [[nodiscard]] VType& get(const FieldNameType& field) const
    {
      DBTABLE_ACCESS_WITH_NAME_EXCEPTION(field);
    }

    [[nodiscard]] VType& get(const FieldIndex& index) const
    {
      DBTABLE_ACCESS_WITH_INDEX_EXCEPTION(index);
    }
  };

  using Object = ObjectImpl<DataIterator, ValueType>;
  using ConstObject = ObjectImpl<ConstDataIterator, const ValueType>;

  /**
   * A proxy class that provides iteration on the table
   * @tparam ObjType
   * @tparam DatumIterator
   */
  template <typename ObjType, typename DatumIterator> class IteratorImpl
  {
    using difference_type = std::ptrdiff_t;
    using value_type = ObjType;
    using pointer = typename ObjType::Ptr;
    using reference = ObjType;
    using iterator_category = std::random_access_iterator_tag;
    // See https://stackoverflow.com/questions/37031805/

    friend class Table;

    DatumIterator it;
    const Table* table = nullptr;

  public:
    IteratorImpl(DatumIterator datumIt, const Table* table_ptr) : it(datumIt), table(table_ptr)
    {
    }

    IteratorImpl() = default;

    IteratorImpl(const IteratorImpl&) = default;

    IteratorImpl(IteratorImpl&&) noexcept = default;

    IteratorImpl& operator=(const IteratorImpl&) = default;

    IteratorImpl& operator=(IteratorImpl&&) noexcept = default;

    ~IteratorImpl() = default;

    pointer operator->()
    {
      return createProxy(it, table);
    }

    reference operator*()
    {
      return *createProxy(it, table);
    }

    pointer operator->() const
    {
      return createProxy(it, table);
    }

    reference operator*() const
    {
      return *createProxy(it, table);
    }

    IteratorImpl operator+(int n)
    {
      return IteratorImpl(it + n, table);
    }

    IteratorImpl operator-(int n)
    {
      return IteratorImpl(it - n, table);
    }

    IteratorImpl& operator+=(int n)
    {
      return it += n, *this;
    }

    IteratorImpl& operator-=(int n)
    {
      return it -= n, *this;
    }

    IteratorImpl& operator++()
    {
      return ++it, *this;
    }

    IteratorImpl& operator--()
    {
      return --it, *this;
    }

    IteratorImpl operator++(int)
    {
      auto retVal = IteratorImpl(*this);
      ++it;
      return retVal;
    }

    IteratorImpl operator--(int)
    {
      auto retVal = IteratorImpl(*this);
      --it;
      return retVal;
    }

    bool operator==(const IteratorImpl& other) const
    {
      return this->it == other.it;
    }

    friend bool operator!=(const IteratorImpl& lhs, const IteratorImpl& rhs)
    {
      return lhs.it != rhs.it;
    }

    bool operator<=(const IteratorImpl& other)
    {
      return this->it <= other.it;
    }

    bool operator>=(const IteratorImpl& other)
    {
      return this->it >= other.it;
    }

    bool operator<(const IteratorImpl& other)
    {
      return this->it < other.it;
    }

    bool operator>(const IteratorImpl& other)
    {
      return this->it > other.it;
    }
  };

  using Iterator = IteratorImpl<Object, decltype(data.begin())>;
  using ConstIterator = IteratorImpl<ConstObject, decltype(data.cbegin())>;

private:
  static ConstObject::Ptr createProxy(ConstDataIterator iterator, const Table* table)
  {
    return std::make_unique<ConstObject>(iterator, table);
  }

  static Object::Ptr createProxy(DataIterator iterator, const Table* table)
  {
    return std::make_unique<Object>(iterator, table);
  }

public:
  Table() = delete;

  explicit Table(std::string name) : tableName(std::move(name))
  {
  }

  /**
   * Accept any container that contains string.
   * @tparam FieldIDContainer
   * @param name: the table name (must be unique in the database)
   * @param fields: an iterable container with fields
   */
  template <class FieldIDContainer> Table(const std::string& name, const FieldIDContainer& fields);

  /**
   * Copy constructor from another table
   * @param name: the table name (must be unique in the database)
   * @param origin: the original table copied from
   */
  Table(std::string name, const Table& origin)
      : fields(origin.fields), fieldMap(origin.fieldMap), data(origin.data), keyMap(origin.keyMap),
        tableName(std::move(name))
  {
  }

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
  void duplicateKeyData(const KeyType& key);

  /**
   * Find the index of a field in the fieldMap
   * @param field
   * @return fieldIndex
   */
  [[nodiscard]] FieldIndex getFieldIndex(const FieldNameType& field) const;

  /**
   * Insert a row of data by its key
   * @tparam ValueTypeContainer
   * @param key
   * @param data
   */
  void insertByIndex(const KeyType& key, std::vector<ValueType>&& data);

  /**
   * Delete a row of data by its key
   * @param key
   */
  void deleteByIndex(const KeyType& key);

  /**
   * Access the value according to the key
   * @param key
   * @return the Object that KEY = key, or nullptr if key doesn't exist
   */
  Object::Ptr operator[](const KeyType& key);

  /**
   * Set the name of the table
   * @param name
   */
  void setName(std::string name)
  {
    this->tableName = std::move(name);
  }

  /**
   * Get the name of the table
   * @return
   */
  [[nodiscard]] const std::string& name() const
  {
    return this->tableName;
  }

  /**
   * Return whether the table is empty
   * @return
   */
  [[nodiscard]] bool empty() const
  {
    return this->data.empty();
  }

  /**
   * Return the num of data stored in the table
   * @return
   */
  [[nodiscard]] size_t size() const
  {
    return this->data.size();
  }

  /**
   * Return the fields in the table
   * @return
   */
  [[nodiscard]] const std::vector<FieldNameType>& field() const
  {
    return this->fields;
  }

  /**
   * Clear all content in the table
   * @return rows affected
   */
  size_t clear()
  {
    auto result = keyMap.size();
    data.clear();
    keyMap.clear();
    return result;
  }

  /**
   * Get a begin iterator similar to the standard iterator
   * @return begin iterator
   */
  Iterator begin()
  {
    return {data.begin(), this};
  }

  /**
   * Get a end iterator similar to the standard iterator
   * @return end iterator
   */
  Iterator end()
  {
    return {data.end(), this};
  }

  /**
   * Get a const begin iterator similar to the standard iterator
   * @return const begin iterator
   */
  [[nodiscard]] ConstIterator begin() const
  {
    return {data.cbegin(), this};
  }

  /**
   * Get a const end iterator similar to the standard iterator
   * @return const end iterator
   */
  [[nodiscard]] ConstIterator end() const
  {
    return {data.cend(), this};
  }

  /**
   * Overload the << operator for complete print of the table
   * @param os
   * @param table
   * @return the origin ostream
   */
  friend std::ostream& operator<<(std::ostream& out, const Table& table);
};

std::ostream& operator<<(std::ostream& out, const Table& table);

template <class FieldIDContainer>
Table::Table(const std::string& name, const FieldIDContainer& fields)
    : fields(fields.cbegin(), fields.cend()), tableName(name)
{
  SizeType index = 0;
  for (const auto& fieldName : fields)
  {
    if (fieldName == "KEY")
      throw MultipleKey("Error creating table \"" + name + "\": Multiple KEY field.");
    fieldMap.emplace(fieldName, index++);
  }
}

#endif // PROJECT_DB_TABLE_H
