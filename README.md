# LemonDB

## Build

Currently the project uses `CMake` for building and `GoogleTest` for testing. Run the following commands to build:

```bash
# configure then build in separate 
cmake -S . -B build
cmake --build build -j4
```

The binary will be located in the `bin` directory, i.e., `bin/lemondb` for the database and `bin/lemondb_tests` for the tests.


## Contributing

This project uses [conventional commits](https://www.conventionalcommits.org) and adheres to [semantic versioning](https://semver.org).

> [!warning]
>
> **For contibutors:**
>
> Please install the git hooks in `hooks` directory by running `./hooks/install-hooks.sh` to ensure conventional commits and non-ASCII characters are avoided.
