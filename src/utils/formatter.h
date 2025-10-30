#ifndef SRC_FORMATTER_H
#define SRC_FORMATTER_H

#include <string>
#include <string_view>
#include <vector>

inline std::string to_string(const std::string& value)
{
  return value;
}

inline std::string to_string(std::string_view value)
{
  return std::string(value);
}

inline std::string to_string(const char* value)
{
  return {value};
}

template <typename T> static inline std::string to_string(const std::vector<T>& vec)
{
  std::string str;
  for (const auto& val : vec)
  {
    str += to_string(val) + " ";
  }
  return str;
}

template <typename T> static inline std::string to_string(const T& value)
{
  return std::to_string(value);
}

template <typename T> inline std::string operator%(std::string format, const T& value)
{
  auto ind = format.find('?');
  if (ind == 0 || format[ind - 1] != '\\')
  {
    format.replace(ind, 1U, to_string(value));
  }
  return format;
}

inline std::string operator%(std::string format, const std::string& value)
{
  auto ind = format.find('?');
  if (ind == 0 || format[ind - 1] != '\\')
  {
    format.replace(ind, 1U, value);
  }
  return format;
}

inline std::string operator%(std::string format, const char* value)
{
  auto ind = format.find('?');
  if (ind == 0 || format[ind - 1] != '\\')
  {
    format.replace(ind, 1U, value);
  }
  return format;
}

inline std::string operator""_f(const char* str, size_t size)
{
  (void)size; // Avoid unused parameter warning
  return {str};
}

#endif // SRC_FORMATTER_H
