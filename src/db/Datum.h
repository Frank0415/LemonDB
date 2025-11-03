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

};