#ifndef SRC_FORMATTER_H
#define SRC_FORMATTER_H

#include <numeric>
#include <string>
#include <string_view>
#include <vector>

/**
 * Convert a string to string (identity)
 */
inline std::string to_string(const std::string &value) { return value; }

/**
 * Convert a string_view to string
 */
inline std::string to_string(std::string_view value) {
  return std::string(value);
}

/**
 * Convert a C-string to string
 */
inline std::string to_string(const char *value) { return {value}; }

/**
 * Convert any type to string using std::to_string
 */
template <typename T> static inline std::string to_string(const T &value) {
  return std::to_string(value);
}

/**
 * Convert a vector to space-separated string
 */
template <typename T>
static inline std::string to_string(const std::vector<T> &vec) {
  return std::accumulate(vec.begin(), vec.end(), std::string(),
                         [](std::string result, const T &element) {
                           return std::move(result) + to_string(element) + " ";
                         });
}

/**
 * Format string by replacing first '?' with value
 * @param format The format string containing '?' placeholders
 * @param value The value to substitute for the first '?'
 * @return Formatted string with substitution applied
 */
template <typename T>
inline std::string operator%(std::string format, const T &value) {
  auto ind = format.find('?');
  if (ind == 0 || format[ind - 1] != '\\') {
    format.replace(ind, 1U, to_string(value));
  }
  return format;
}

/**
 * Format string by replacing first '?' with string value
 * @param format The format string containing '?' placeholders
 * @param value The string value to substitute for the first '?'
 * @return Formatted string with substitution applied
 */
inline std::string operator%(std::string format, const std::string &value) {
  auto ind = format.find('?');
  if (ind == 0 || format[ind - 1] != '\\') {
    format.replace(ind, 1U, value);
  }
  return format;
}

/**
 * Format string by replacing first '?' with C-string value
 * @param format The format string containing '?' placeholders
 * @param value The C-string value to substitute for the first '?'
 * @return Formatted string with substitution applied
 */
inline std::string operator%(std::string format, const char *value) {
  auto ind = format.find('?');
  if (ind == 0 || format[ind - 1] != '\\') {
    format.replace(ind, 1U, value);
  }
  return format;
}

/**
 * User-defined literal for format strings
 */
inline std::string operator""_f(const char *str, size_t size) {
  (void)size;  // Avoid unused parameter warning
  return {str};
}

#endif  // SRC_FORMATTER_H
