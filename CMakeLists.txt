cmake_minimum_required(VERSION 3.12)#3.12 needed for CONFIGURE_DEPENDS
project(lemondb VERSION 1.0.0 LANGUAGES CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Set output directories
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
# Common compile options
set(LEMONDB_COMMON_COMPILE_OPTIONS
    -Wall
    -Wextra
    -Werror
    -pedantic
    -Wno-unused-result
    -Wconversion
    -Wvla
    -O2
)

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
file(GLOB MAIN_SOURCES CONFIGURE_DEPENDS
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
file(GLOB LIB_SOURCES_TEMP CONFIGURE_DEPENDS
    src/db/*.cpp
    # src/db/*.h
    src/query/*.cpp
    # src/query/*.h
    src/query/data/*.cpp
    # src/query/data/*.h
    src/query/management/*.cpp
    # src/query/management/*.h
    src/threading/*.cpp
    # src/threading/*.h
    # src/utils/*.h
)
set(LIB_SOURCES ${LIB_SOURCES_TEMP})


# Create a library for testing
add_library(lemondb_lib STATIC ${LIB_SOURCES})
target_include_directories(lemondb_lib PUBLIC ${CMAKE_SOURCE_DIR}/src)

# Create the main executable
add_executable(lemondb ${MAIN_SOURCES})
target_link_libraries(lemondb PRIVATE lemondb_lib)

# Set compiler options for the library and executable
target_compile_options(lemondb PRIVATE
    ${LEMONDB_COMMON_COMPILE_OPTIONS}
)
target_compile_options(lemondb_lib PRIVATE
    ${LEMONDB_COMMON_COMPILE_OPTIONS}
)

# Options to build sanitizer variants (ASAN/MSAN/UBSAN/TSAN)
option(ENABLE_ASAN "Build AddressSanitizer instrumented binaries" OFF)
option(ENABLE_MSAN "Build MemorySanitizer instrumented binaries" OFF)
option(ENABLE_UBSAN "Build UndefinedBehaviorSanitizer instrumented binaries" OFF)
option(ENABLE_TSAN "Build ThreadSanitizer instrumented binaries" OFF)

# Profiling options
option(ENABLE_GPROF "Enable GProf profiling" OFF)
option(ENABLE_CALLGRIND "Enable Callgrind-compatible profiling" OFF)
option(ENABLE_GPERFTOOLS "Enable gperftools (Google profiler) support" OFF)

if(ENABLE_ASAN)
    message(STATUS "Configuring ASAN build targets")
    set(LEMONDB_ASAN_FLAGS -fsanitize=address -fno-omit-frame-pointer -fno-sanitize-recover=all -g)
    add_library(lemondb_lib_asan STATIC ${LIB_SOURCES})
    target_include_directories(lemondb_lib_asan PUBLIC ${CMAKE_SOURCE_DIR}/src)
    target_compile_options(lemondb_lib_asan PRIVATE ${LEMONDB_COMMON_COMPILE_OPTIONS} ${LEMONDB_ASAN_FLAGS})
    # Ensure sanitizer flags are passed at link time as well
    target_link_options(lemondb_lib_asan PRIVATE ${LEMONDB_ASAN_FLAGS})

    add_executable(lemondb_asan ${MAIN_SOURCES})
    target_link_libraries(lemondb_asan PRIVATE lemondb_lib_asan)
    target_compile_options(lemondb_asan PRIVATE ${LEMONDB_COMMON_COMPILE_OPTIONS} ${LEMONDB_ASAN_FLAGS})
    target_link_options(lemondb_asan PRIVATE ${LEMONDB_ASAN_FLAGS})
endif()

if(ENABLE_MSAN)
    if(NOT CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        message(FATAL_ERROR "MemorySanitizer (MSAN) requires Clang/LLVM. Re-run CMake with -DCMAKE_CXX_COMPILER=clang++ or disable ENABLE_MSAN.")
    endif()
    message(STATUS "Configuring MSAN build targets")
    set(LEMONDB_MSAN_FLAGS -fsanitize=memory -fno-omit-frame-pointer -fno-optimize-sibling-calls -g -fsanitize-memory-track-origins)
    add_library(lemondb_lib_msan STATIC ${LIB_SOURCES})
    target_include_directories(lemondb_lib_msan PUBLIC ${CMAKE_SOURCE_DIR}/src)
    target_compile_options(lemondb_lib_msan PRIVATE ${LEMONDB_COMMON_COMPILE_OPTIONS} ${LEMONDB_MSAN_FLAGS})
    # Pass MSAN flags to the linker too (required to link MSAN runtime)
    target_link_options(lemondb_lib_msan PRIVATE ${LEMONDB_MSAN_FLAGS})

    add_executable(lemondb_msan ${MAIN_SOURCES})
    target_link_libraries(lemondb_msan PRIVATE lemondb_lib_msan)
    target_compile_options(lemondb_msan PRIVATE ${LEMONDB_COMMON_COMPILE_OPTIONS} ${LEMONDB_MSAN_FLAGS})
    target_link_options(lemondb_msan PRIVATE ${LEMONDB_MSAN_FLAGS})
endif()

if(ENABLE_UBSAN)
    if(NOT CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        message(FATAL_ERROR "UndefinedBehaviorSanitizer subsanitizers used here require Clang/LLVM. Re-run CMake with -DCMAKE_CXX_COMPILER=clang++ or disable ENABLE_UBSAN.")
    endif()
    message(STATUS "Configuring UBSAN build targets")
    set(LEMONDB_UBSAN_FLAGS -fsanitize=undefined,unsigned-integer-overflow -g)
    add_library(lemondb_lib_ubsan STATIC ${LIB_SOURCES})
    target_include_directories(lemondb_lib_ubsan PUBLIC ${CMAKE_SOURCE_DIR}/src)
    target_compile_options(lemondb_lib_ubsan PRIVATE ${LEMONDB_COMMON_COMPILE_OPTIONS} ${LEMONDB_UBSAN_FLAGS})
    target_link_options(lemondb_lib_ubsan PRIVATE ${LEMONDB_UBSAN_FLAGS})

    add_executable(lemondb_ubsan ${MAIN_SOURCES})
    target_link_libraries(lemondb_ubsan PRIVATE lemondb_lib_ubsan)
    target_compile_options(lemondb_ubsan PRIVATE ${LEMONDB_COMMON_COMPILE_OPTIONS} ${LEMONDB_UBSAN_FLAGS})
    target_link_options(lemondb_ubsan PRIVATE ${LEMONDB_UBSAN_FLAGS})
endif()

if(ENABLE_TSAN)
    message(STATUS "Configuring TSAN build targets")
    set(LEMONDB_TSAN_FLAGS -fsanitize=thread -g)
    add_library(lemondb_lib_tsan STATIC ${LIB_SOURCES})
    target_include_directories(lemondb_lib_tsan PUBLIC ${CMAKE_SOURCE_DIR}/src)
    target_compile_options(lemondb_lib_tsan PRIVATE ${LEMONDB_COMMON_COMPILE_OPTIONS} ${LEMONDB_TSAN_FLAGS})
    target_link_options(lemondb_lib_tsan PRIVATE ${LEMONDB_TSAN_FLAGS})

    add_executable(lemondb_tsan ${MAIN_SOURCES})
    target_link_libraries(lemondb_tsan PRIVATE lemondb_lib_tsan)
    target_compile_options(lemondb_tsan PRIVATE ${LEMONDB_COMMON_COMPILE_OPTIONS} ${LEMONDB_TSAN_FLAGS})
    target_link_options(lemondb_tsan PRIVATE ${LEMONDB_TSAN_FLAGS})
endif()

# Profiling flags
set(PROFILING_FLAGS "")
if(ENABLE_GPROF)
    list(APPEND PROFILING_FLAGS -pg)
endif()
if(ENABLE_CALLGRIND OR CMAKE_BUILD_TYPE STREQUAL "Debug")
    list(APPEND PROFILING_FLAGS -g)
endif()
if(ENABLE_GPERFTOOLS)
    list(APPEND PROFILING_FLAGS -g)
    # try to link profiler library for convenience when enabled
    find_library(GPERFTOOLS_PROFILER profiler)
    if(GPERFTOOLS_PROFILER)
        target_link_libraries(lemondb_lib PUBLIC ${GPERFTOOLS_PROFILER})
    endif()
endif()

# Apply profiling flags to main targets
target_compile_options(lemondb_lib PRIVATE ${PROFILING_FLAGS})
target_compile_options(lemondb PRIVATE ${PROFILING_FLAGS})

# Also apply to sanitizer variants if profiling is enabled
if(ENABLE_ASAN)
    target_compile_options(lemondb_lib_asan PRIVATE ${PROFILING_FLAGS})
    target_compile_options(lemondb_asan PRIVATE ${PROFILING_FLAGS})
endif()
if(ENABLE_MSAN)
    target_compile_options(lemondb_lib_msan PRIVATE ${PROFILING_FLAGS})
    target_compile_options(lemondb_msan PRIVATE ${PROFILING_FLAGS})
endif()
if(ENABLE_UBSAN)
    target_compile_options(lemondb_lib_ubsan PRIVATE ${PROFILING_FLAGS})
    target_compile_options(lemondb_ubsan PRIVATE ${PROFILING_FLAGS})
endif()
if(ENABLE_TSAN)
    target_compile_options(lemondb_lib_tsan PRIVATE ${PROFILING_FLAGS})
    target_compile_options(lemondb_tsan PRIVATE ${PROFILING_FLAGS})
endif()

# Google Test Setup
# include(FetchContent)
# FetchContent_Declare(
#     googletest
#     URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
#     DOWNLOAD_EXTRACT_TIMESTAMP TRUE
# )

# For Windows: Prevent overriding the parent project's compiler/linker settings
# set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

# FetchContent_MakeAvailable(googletest)

# Enable testing
# enable_testing()

# Create test executable
# add_executable(lemondb_tests
#     test/gtests/test_example.cpp
#     test/gtests/test_count_query.cpp
#     test/gtests/test_delete_query.cpp
#     test/gtests/test_limits_query.cpp
#     test/gtests/test_select_query.cpp
#     test/gtests/test_duplicate_query.cpp
#     test/gtests/test_swap_query.cpp
#     test/gtests/test_arith_query.cpp
#     test/gtests/test_sum_query.cpp
#     test/gtests/test_copytable_query.cpp
#     test/gtests/test_truncate_query.cpp
# )

# target_include_directories(lemondb_tests PRIVATE ${CMAKE_SOURCE_DIR}/src)
# target_link_libraries(lemondb_tests 
#     lemondb_lib
#     gtest
#     gtest_main
# )

# Register tests with gtest
# add_test(NAME lemondb_tests COMMAND lemondb_tests)