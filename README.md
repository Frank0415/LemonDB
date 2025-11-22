# LemonDB

![Build Status](https://focs.ji.sjtu.edu.cn/git/ece482/p2team01/actions/workflows/push.yaml/badge.svg)
![Language](https://img.shields.io/badge/language-C%2B%2B20-blue.svg)
![Build](https://img.shields.io/badge/build-CMake-green.svg)

**LemonDB** is a high-performance, in-memory database management system designed for speed and efficiency. It supports a custom SQL-like query language, parallel query execution, and robust error handling.

## Features

- **High Performance**: Optimized for in-memory operations with multi-threaded query execution.
- **Parallel Processing**: Utilizes a custom thread pool and query manager for concurrent table operations.
- **Robust Query Language**: Supports a wide range of operations including `LOAD`, `DUMP`, `SELECT`, `INSERT`, `UPDATE`, `DELETE`, and aggregations (`SUM`, `MIN`, `MAX`).
- **Interactive & Batch Modes**: Run queries interactively via standard input or execute batch scripts using `LISTEN`.
- **Advanced Debugging**: Built-in support for AddressSanitizer (ASan), MemorySanitizer (MSan), ThreadSanitizer (TSan), and UndefinedBehaviorSanitizer (UBSan).

## User Guide

### Prerequisites

- **C++ Compiler**: Clang 18+ (C++20 support required).
- **CMake**: Version 3.16 or higher.
- **Make** or **Ninja**.

### Build Instructions

1.  **Clone the repository:**
    ```bash
    git clone <repository-url> ./lemondb
    cd lemondb
    ```

2.  **Configure and Build:**
    ```bash
    cmake -S . -B build
    cmake --build build -j$(nproc)
    ```

    The binary will be created at `build/bin/lemondb`.

### Usage

#### Running the Database

Start the database in interactive mode:
```bash
./build/bin/lemondb
```

Run with a listen file (batch mode):
```bash
./build/bin/lemondb --listen path/to/query_file.query
```

#### Command Line Arguments

-   `--listen <file>` or `-l <file>`: Execute commands from the specified file.
-   `--threads <num>` or `-t <num>`: Set the number of worker threads (default: hardware concurrency).

#### Query Examples

For a complete reference of supported commands and syntax, please refer to the **[Query Syntax Reference](https://focs.ji.sjtu.edu.cn/git/ece482/p2team01/wiki/Query-Syntax-Reference)** in the Wiki.

```sql
-- Load a table from a file
LOAD db/users.tbl;

-- Select specific columns
SELECT ( id name age ) FROM users WHERE ( age > 18 );

-- Insert data
INSERT ( 1 "Alice" 25 ) FROM users;

-- Aggregation
SUM ( salary ) FROM employees;

-- Nested execution
LISTEN other_queries.query;

-- Exit
QUIT;
```

## Developer Guide

For comprehensive development guidelines, coding standards, and contribution rules, please refer to the **[Developer Guidelines](https://focs.ji.sjtu.edu.cn/git/ece482/p2team01/wiki/Developer-Guidelines)** in our Wiki.

### Building with Sanitizers

For development and debugging, you can build with sanitizers enabled:

-   **AddressSanitizer (ASan):**
    ```bash
    cmake -S . -B build -DENABLE_ASAN=ON
    cmake --build build -j$(nproc)
    ```

-   **MemorySanitizer (MSan):**
    Requires Clang and an MSan-instrumented `libc++`.
    
    **Note:** The project is configured to look for the instrumented `libc++` at `/usr/local/lib/libc++_msan-18`. If your installation is different, you must modify `CMakeLists.txt` or ensure the library is available at that path.

    ```bash
    cmake -S . -B build -DCMAKE_CXX_COMPILER=clang++ -DENABLE_MSAN=ON
    cmake --build build -j$(nproc)
    ```

-   **ThreadSanitizer (TSan):**
    Detects data races in multi-threaded code.
    ```bash
    cmake -S . -B build -DENABLE_TSAN=ON
    cmake --build build -j$(nproc)
    ```

-   **UndefinedBehaviorSanitizer (UBSan):**
    ```bash
    cmake -S . -B build -DCMAKE_CXX_COMPILER=clang++ -DENABLE_UBSAN=ON
    cmake --build build -j$(nproc)
    ```

### Testing & Quality

#### Running Tests

We use a custom test suite to ensure correctness.

```bash
./test/run.sh
```

#### Static Analysis

The project enforces code quality using `clang-format`, `clang-tidy`, and `cppcheck`.

```bash
./scripts/run-static-analysis.sh
```

### Contributing

This project uses [conventional commits](https://www.conventionalcommits.org) and adheres to [semantic versioning](https://semver.org).

> [!warning]
>
> **For contibutors:**
>
> Please install the git hooks in `hooks` directory by running `./hooks/install-hooks.sh` to ensure conventional commits and non-ASCII characters are avoided.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
