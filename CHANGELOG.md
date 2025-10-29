# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to milestone versioning.

## p2m1

### Added
- Implemented all required query types:
  - SELECT, DELETE, SWAP, MAX, MIN, SUM, AVERAGE, COUNT, DUPLICATE and more
  - Complete table management operations (LOAD, DUMP, DROP, LIST, PRINT, QUIT)
  - Table structure preservation and validation
  - Error handling and validation for all operations

- Build & Quality System:
  - CMake-based build system with C++20 support
  - Comprehensive test suite using GoogleTest
  - Sanitizer builds (ASAN, MSAN, UBSAN) support
  - Static analysis integration (clang-tidy, cppcheck, cpplint)
  - Code formatting with clang-format
  - Pre-commit hooks for quality enforcement
  - CI/CD workflows

### Testing
- Comprehensive unit test suite
- Integration test framework with query/expected output comparison
- Test coverage for all query types and edge cases
- Performance benchmarking framework

### Changed
- Updated to C++20 standard
- Enhanced error handling with custom exception types
- Improved and changed query parsing and result formatting

### Fixed
- Various code quality issues identified by static analysis
- Memory leaks and undefined behaviors detected by sanitizers
- Bugs in query execution logic uncovered during testing