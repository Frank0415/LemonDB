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
cmake --build build -j4
```
or
```bash
# configure and build together 
cmake -S . -B build && cmake --build build -j4
```
> 'Reconfigure only when CMake files or target lists change (or when **using `GLOB` without `CONFIGURE_DEPENDS` and you add/remove sources**). Editing existing sources only needs a build.'

The binary will be located in the `bin` directory, i.e., `bin/lemondb` for the database and `bin/lemondb_tests` for the tests.

## Static Analysis

Every push runs clang-format, clang-tidy, cppcheck, and cpplint in CI. You can reproduce the same checks locally with:

```bash
./scripts/run-static-analysis.sh
```

Make sure the script's dependencies (`cmake`, `clang-format`, `clang-tidy`, `cppcheck`, and the Python `cpplint` package) are installed and on your `PATH`.


## Contributing

This project uses [conventional commits](https://www.conventionalcommits.org) and adheres to [semantic versioning](https://semver.org).

> [!warning]
>
> **For contibutors:**
>
> Please install the git hooks in `hooks` directory by running `./hooks/install-hooks.sh` to ensure conventional commits and non-ASCII characters are avoided.
