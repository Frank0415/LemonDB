# LemonDB

## Build

Currently the project uses `CMake` for building and `GoogleTest` for testing. Run the following commands to build:

```bash
# configure then build in separate 
cmake -S . -B build
#-j4 means you assign 4 cpu cores to the compiler. 
#Change the number as you want 
#Or use -j$(nproc) to use as many as cores.
#(-j0 if you use ninja instead of make) 
cmake --build build -j8
```
or
```bash
# configure and build together 
cmake -S . -B build && cmake --build build -j8
```
> 'Reconfigure only when CMake files or target lists change (or when **using `GLOB` without `CONFIGURE_DEPENDS` and you add/remove sources**). Editing existing sources only needs a build.'

The binary will be located in the `bin` directory, i.e., `bin/lemondb` for the database and `bin/lemondb_tests` for the tests.

## Static Analysis

Every push runs clang-format, clang-tidy, cppcheck, and cpplint in CI.
A comprehensive static analysis tool is automatically run on every commit and pull request via Gitea Actions, using the `./test/run.sh` script.

Make sure the script's dependencies (`cmake`, `clang-format`, `clang-tidy`, `cppcheck`, and the Python `cpplint` package) are installed and on your `PATH`.

## Performance Benchmarking

### Single-threaded Benchmarking 

```bash
Test: single_read completed in 29.259891043 seconds
PASS: single_read
Test: single_read_dup completed in 32.502383623 seconds
PASS: single_read_dup
Test: single_read_update completed in 26.000829756 seconds
PASS: single_read_update
Test: single_insert_delete completed in 21.017652290 seconds
PASS: single_insert_delete
Test: few_read completed in 17.914737771 seconds
PASS: few_read
Test: few_read_dup completed in 82.905387144 seconds
PASS: few_read_dup
Test: few_read_update completed in 17.895444505 seconds
PASS: few_read_update
Test: few_insert_delete completed in 13.260974489 seconds
PASS: few_insert_delete
Test: many_read completed in 17.690794110 seconds
PASS: many_read
Test: many_read_dup completed in 22.463446368 seconds
PASS: many_read_dup
Test: many_read_update completed in 19.056095016 seconds
PASS: many_read_update
Test: many_insert_delete completed in 17.194186674 seconds
PASS: many_insert_delete
Test: test completed in 1.816182835 seconds
PASS: test
```


## Contributing

This project uses [conventional commits](https://www.conventionalcommits.org) and adheres to [semantic versioning](https://semver.org).

> [!warning]
>
> **For contibutors:**
>
> Please install the git hooks in `hooks` directory by running `./hooks/install-hooks.sh` to ensure conventional commits and non-ASCII characters are avoided.
