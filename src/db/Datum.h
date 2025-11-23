#ifndef PROJECT_DB_DATUM_H
#define PROJECT_DB_DATUM_H

#include <cstddef>
#include <string>
#include <vector>
#include <utility>

/**
 * Represents a single record (datum) consisting of a unique string key and
 * an ordered sequence of integer field values.
 *
 * Provides lightweight value semantics (all special member functions defaulted)
 * and multiple constructors for flexible initialization from container types
 * or rvalue vectors. Accessors expose references (const and non-const) to
 * avoid unnecessary copying while keeping member variables encapsulated.
 */
class Datum {
private:
  /** Unique key identifying this datum (e.g., primary key). */
  std::string key;
  /** Field values stored in positional order matching table schema. */
  std::vector<int> datum;

public:
  /**
   * Default constructor creates an empty datum with no key and no values.
   */
  Datum() = default;

  // By declaring all 5 special member functions, we adhere to the Rule of
  // Five.
  Datum(const Datum &) = default;
  Datum &operator=(const Datum &) =
      default;  // Fix: Explicitly default the copy assignment operator.
  Datum(Datum &&) noexcept = default;
  Datum &operator=(Datum &&) noexcept = default;
  ~Datum() = default;

  /**
   * Construct a datum with a given number of integer fields
   * (value-initialized).
   * @param size Number of fields to allocate.
   */
  explicit Datum(const size_t &size) : datum(size, int()) {}

  /**
   * Construct a datum from a generic integer container (copied).
   * @tparam intContainer Iterable container with integral elements.
   * @param key Unique key string.
   * @param datum Container whose contents are copied into internal vector.
   */
  template <class intContainer>
  explicit Datum(std::string key, const intContainer &datum)
      : key(std::move(key)), datum(datum) {}

  /**
   * Construct a datum by moving an existing vector of field values.
   * @param key Unique key string.
   * @param datum Rvalue vector of field values (moved).
   */
  explicit Datum(std::string key, std::vector<int> &&datum) noexcept
      : key(std::move(key)), datum(std::move(datum)) {}

  /**
   * Get const reference to the key.
   * @return Const reference to key string.
   */
  [[nodiscard]] const std::string &keyConstRef() const noexcept { return key; }

  /**
   * Get mutable reference to the key (allowing modification).
   * @return Mutable reference to key string.
   */
  [[nodiscard]] std::string &keyRef() noexcept { return key; }

  /**
   * Set / replace the key value.
   * @param newKey New key string (moved in).
   */
  void setKey(std::string newKey) noexcept { key = std::move(newKey); }

  /**
   * Get const reference to the vector of field values.
   * @return Const reference to internal values vector.
   */
  [[nodiscard]] const std::vector<int> &datumConstRef() const noexcept {
    return datum;
  }

  /**
   * Get mutable reference to the vector of field values.
   * @return Mutable reference to internal values vector.
   */
  [[nodiscard]] std::vector<int> &datumRef() noexcept { return datum; }
};

#endif  // PROJECT_DB_DATUM_H