#ifndef PROJECT_DB_DATUM_H
#define PROJECT_DB_DATUM_H

#include <string>
#include <vector>

class Datum
{
private:
  /** Unique key of this datum */
  std::string key;
  /** The values in the order of fields */
  std::vector<int> datum;

public:
  Datum() = default;

  // By declaring all 5 special member functions, we adhere to the Rule of
  // Five.
  Datum(const Datum&) = default;
  Datum& operator=(const Datum&) = default; // Fix: Explicitly default the copy assignment operator.
  Datum(Datum&&) noexcept = default;
  Datum& operator=(Datum&&) noexcept = default;
  ~Datum() = default;

  explicit Datum(const size_t& size) : datum(size, int())
  {
  }

  template <class intContainer>
  explicit Datum(std::string key, const intContainer& datum) : key(std::move(key)), datum(datum)
  {
  }

  explicit Datum(std::string key, std::vector<int>&& datum) noexcept
      : key(std::move(key)), datum(std::move(datum))
  {
  }

  // Accessors so outer code need not access members directly
  [[nodiscard]] const std::string& keyConstRef() const noexcept
  {
    return key;
  }

  [[nodiscard]] std::string& keyRef() noexcept
  {
    return key;
  }

  void setKey(std::string newKey) noexcept
  {
    key = std::move(newKey);
  }

  [[nodiscard]] const std::vector<int>& datumConstRef() const noexcept
  {
    return datum;
  }

  [[nodiscard]] std::vector<int>& datumRef() noexcept
  {
    return datum;
  }
};

#endif // PROJECT_DB_DATUM_H