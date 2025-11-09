# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to milestone versioning.

## p2m2

### Added
- Multi-threaded query execution infrastructure:
  - Thread pool implementation (`ThreadPool`) for concurrent task execution
  - Worker threads managed via singleton pattern with configurable thread count
  - Automatic thread count detection using `std::thread::hardware_concurrency()`
  - Query result collector (`QueryResultCollector`) for asynchronous result aggregation
  - Mutex-based synchronization for thread-safe result collection
  
- Command-line argument parsing:
  - `--threads=<num>` or `-t <num>` to specify number of worker threads
  - `--listen=<file>` or `-l <file>` to specify input file path
  - Automatic validation and error handling for arguments
  - Support for both long-form (`--option=value`) and short-form (`-o value`) syntax

- Input handling:
  - File-based input via `--listen` argument
  - Standard input (stdin) support in debug mode when no file specified
  - Production mode requires explicit `--listen` argument for safety
  - Query extraction with semicolon-based parsing

- Query execution model:
  - Sequential query execution ensuring data consistency
  - Internal parallelism within queries using thread pool
  - Asynchronous result collection with ordered output
  - Query ID tracking for proper result sequencing
  - Automatic QUIT query detection and handling

### Implementation Details
- Thread pool uses condition variables for efficient task scheduling
- Idle thread monitoring for performance tracking
- Result collector maintains insertion order using `std::map`
- All standard query operations supported (SELECT, INSERT, DELETE, UPDATE, etc.)
- Debug output for query execution tracking

### Technical Architecture
- Singleton pattern for global thread pool instance
- RAII-based resource management in thread pool
- Lock-based mutual exclusion for shared data structures
- Future-based task submission for type-safe async operations

### Known Limitations  
- Network-based input not implemented (no socket LISTEN functionality)
- Queries execute sequentially to prevent data races (parallel execution planned for future)

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