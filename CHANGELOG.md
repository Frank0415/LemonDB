# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to milestone versioning.

## [p2m3] - 2025-11-22

### Added
- **Full `LISTEN` Command Support**:
  - Implemented nested `LISTEN` capabilities for executing batch queries from files.
  - Added immediate result output for `LISTEN` queries to improve user feedback.
  - Ensured correct execution order and dependency management for nested scripts.
- **Table-Level Parallelism**:
  - Implemented asynchronous execution for `DELETE` and `COPYTABLE` operations.
  - Introduced a new `QueryManager` architecture to handle table-level dependencies.
  - Added `StreamOutputPool` and interval decider for efficient, non-blocking output handling.
- **Optimizations**:
  - Implemented short-circuit evaluation for query conditions.
  - Optimized 'Key' string comparisons to improve query performance.
  - **Small Workload Optimization**:
    - Added automatic detection for small workloads (low line count, no nested `LISTEN`).
    - Bypasses thread pool initialization for small tasks to reduce startup overhead.
- **Documentation & Build**:
  - Modernized `README.md` with badges and clearer instructions.
  - Added comprehensive documentation for core header files (`MainUtils.h`, `Datum.h`, `TableLockManager.h`, etc.).
  - Added support for ThreadSanitizer (TSan) in CMake configuration.
  - Enhanced MSAN support with custom `libc++` paths.

### Fixed
- Resolved data race issues using locks and semaphores.
- Fixed `LISTEN` command execution order to ensure deterministic behavior.
- Addressed various code quality issues and clang-format inconsistencies.
- Fixed MSAN build configuration for local testing.

## [p2m2] - 2025-11-09

### Added
- **Multi-threaded Infrastructure**:
  - Implemented a singleton `ThreadPool` for concurrent task execution.
  - Added `QueryResultCollector` for asynchronous result aggregation.
  - Implemented table-level Read-Write locks (`TableLockManager`) to ensure data safety.
- **Async Query Implementation**:
  - Converted major query types to asynchronous execution: `SELECT`, `INSERT`, `UPDATE`, `SUM`, `MAX`, `MIN`, `COUNT`, `DUPLICATE`, `SWAP`.
  - Implemented batch loading support for improved performance.
- **Tooling**:
  - Added performance profiling scripts using `valgrind` and `gprof`.
  - Enhanced command-line argument parsing for thread configuration.

### Fixed
- Fixed memory leaks and undefined behaviors detected by MSAN.
- Improved script quality and error handling.
- Fixed batch load implementation issues.

## [p2m1] - 2025-10-29

### Added
- **Core Query Functionality**:
  - Implemented basic query operations: `SELECT`, `DELETE`, `MAX`, `MIN`, `SUM`, `COUNT`, `DUPLICATE`, `SWAP`, `ADD`, `SUB`.
  - Implemented table management operations: `LOAD`, `DUMP`, `TRUNCATE`, `COPYTABLE`.
- **System Architecture**:
  - Established CMake-based build system with C++20 support.
  - Implemented robust error handling with custom exception classes.
- **Quality Assurance**:
  - Integrated CI/CD workflows for `clang-format`, `cpplint`, and sanitizers (ASAN, MSAN, UBSAN).
  - Added pre-commit hooks for code quality enforcement.
  - Created comprehensive unit and integration test suites.

### Fixed
- Fixed various compiler warnings and errors.
- Resolved clang-tidy and clang-format issues.
- Fixed bugs in `DELETE` conditional logic and iterator handling.
