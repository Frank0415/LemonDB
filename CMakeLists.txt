cmake_minimum_required(VERSION 3.10)
project(lemondb VERSION 1.0.0 LANGUAGES CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Set output directories
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# Comprehensive compiler flags (based on Makefile)
# -O2: Optimization level
# -Wall -Wextra -Werror -pedantic: Strict warnings
# -Wno-unused-result: Suppress unused result warnings
# -Wconversion: Warn on implicit conversions
# -Wvla: Warn on variable length arrays
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++20 -Wall -Wextra -Werror -pedantic -Wno-unused-result -Wconversion -Wvla")

# add_compile_options(
#     -Wall
#     -Wextra
#     -Werror
#     -pedantic
#     -Wno-unused-result
#     -Wconversion
#     -Wvla
#     -O2
# )


# Define source files for the main application using glob patterns
file(GLOB MAIN_SOURCES
    src/main.cpp
    # src/db/*.cpp
    # src/db/*.h
    # src/query/*.cpp
    # src/query/*.h
    # src/query/data/*.cpp
    # src/query/data/*.h
    # src/query/management/*.cpp
    # src/query/management/*.h
    # src/utils/*.h
)

# Define library sources (excluding main.cpp)
file(GLOB LIB_SOURCES_TEMP
    src/db/*.cpp
    # src/db/*.h
    src/query/*.cpp
    # src/query/*.h
    src/query/data/*.cpp
    # src/query/data/*.h
    src/query/management/*.cpp
    # src/query/management/*.h
    # src/utils/*.h
)
set(LIB_SOURCES ${LIB_SOURCES_TEMP})

# Create the main executable
add_executable(lemondb ${MAIN_SOURCES})
target_link_libraries(lemondb PRIVATE lemondb_lib)

# Create a library for testing
add_library(lemondb_lib STATIC ${LIB_SOURCES})
target_include_directories(lemondb_lib PUBLIC ${CMAKE_SOURCE_DIR}/src)

# Set compiler options for the library and executable
target_compile_options(lemondb PRIVATE
    -Wall
    -Wextra
    -Werror
    -pedantic
    -Wno-unused-result
    -Wconversion
    -Wvla
    -O2
)
target_compile_options(lemondb_lib PUBLIC
    -Wall
    -Wextra
    -Werror
    -pedantic
    -Wno-unused-result
    -Wconversion
    -Wvla
    -O2
)

# Google Test Setup
include(FetchContent)
FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)

# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(googletest)

# Enable testing
enable_testing()

# Create test executable
add_executable(lemondb_tests
    tests/test_example.cpp
)

target_include_directories(lemondb_tests PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(lemondb_tests 
    lemondb_lib
    gtest
    gtest_main
)

# Register tests with gtest
add_test(NAME lemondb_tests COMMAND lemondb_tests)