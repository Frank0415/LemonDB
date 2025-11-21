#!/bin/bash

# Parse command line arguments
FULL_COMPARE=false
VERBOSE=false
ENABLE_VALGRIND=false
ENABLE_GPROF=false
ENABLE_GPERFTOOLS=false
ENABLE_PROF=false
ENABLE_ASAN=false
ENABLE_MSAN=false
ENABLE_TSAN=false
ENABLE_UBSAN=false

for arg in "$@"; do
    case "$arg" in
        --full|-f)
            FULL_COMPARE=true
            ;;
        --verbose|-v)
            VERBOSE=true
            ;;
        --valgrind)
            ENABLE_VALGRIND=true
            ;;
        --gprof)
            # kept as alias for backward-compatibility -> gperftools
            ENABLE_GPROF=true
            ;;
        --gperftools)
            ENABLE_GPERFTOOLS=true
            ;;
        --prof)
            ENABLE_PROF=true
            ;;
        --asan)
            ENABLE_ASAN=true
            ;;
        --msan)
            ENABLE_MSAN=true
            ;;
        --tsan)
            ENABLE_TSAN=true
            ;;
        --ubsan)
            ENABLE_UBSAN=true
            ;;
        --help|-h)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --full, -f          Enable full comparison mode"
            echo "  --verbose, -v       Enable verbose output"
            echo "  --valgrind          Enable Valgrind/Callgrind profiling"
            echo "  --gprof             Enable GProf profiling"
            echo "  --gperftools        Enable gperftools profiling"
            echo "  --prof              Enable both Valgrind and gperftools profiling"
            echo "  --asan              Enable AddressSanitizer"
            echo "  --msan              Enable MemorySanitizer"
            echo "  --tsan              Enable ThreadSanitizer"
            echo "  --ubsan             Enable UndefinedBehaviorSanitizer"
            echo "  --help, -h          Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0 --verbose --full"
            echo "  $0 --valgrind"
            echo "  $0 --asan"
            exit 0
            ;;
    esac
done

# Check for mutually exclusive sanitizers
SANITIZER_COUNT=0
[ "$ENABLE_ASAN" = true ] && ((SANITIZER_COUNT++))
[ "$ENABLE_MSAN" = true ] && ((SANITIZER_COUNT++))
[ "$ENABLE_TSAN" = true ] && ((SANITIZER_COUNT++))
# UBSAN can technically run with ASAN/TSAN, but for simplicity and binary selection we treat it as separate mode here,
# or we could allow it. But CMakeLists creates separate binaries. So we enforce one at a time for this script.
[ "$ENABLE_UBSAN" = true ] && ((SANITIZER_COUNT++))

if [ $SANITIZER_COUNT -gt 1 ]; then
    echo "Error: Please enable only one sanitizer at a time (--asan, --msan, --tsan, --ubsan)."
    exit 1
fi

if [ "$VERBOSE" = true ]; then
    echo "Verbose mode enabled"
    [ "$FULL_COMPARE" = true ] && echo "Full comparison mode enabled"
    [ "$ENABLE_VALGRIND" = true ] && echo "Valgrind profiling enabled"
    [ "$ENABLE_GPROF" = true ] && echo "GProf is not working"
    [ "$ENABLE_GPERFTOOLS" = true ] && echo "gperftools profiling enabled"
    [ "$ENABLE_PROF" = true ] && echo "Profiling (valgrind + gperftools) enabled"
    [ "$ENABLE_ASAN" = true ] && echo "AddressSanitizer enabled"
    [ "$ENABLE_MSAN" = true ] && echo "MemorySanitizer enabled"
    [ "$ENABLE_TSAN" = true ] && echo "ThreadSanitizer enabled"
    [ "$ENABLE_UBSAN" = true ] && echo "UndefinedBehaviorSanitizer enabled"
    echo ""
fi

if [ "$ENABLE_VALGRIND" != true ] && [ "$ENABLE_GPROF" != true ] && [ "$ENABLE_GPERFTOOLS" != true ] && [ "$ENABLE_PROF" != true ]; then
    ./test/linecount.sh --verbose=$VERBOSE
fi

usr=$(whoami)

# Build profiling flags
PROFILING_FLAGS=""
if [ "$ENABLE_VALGRIND" = true ] || [ "$ENABLE_PROF" = true ]; then
    PROFILING_FLAGS="$PROFILING_FLAGS -DENABLE_CALLGRIND=ON -DCMAKE_BUILD_TYPE=Debug"
fi
if [ "$ENABLE_GPROF" = true ]|| [ "$ENABLE_PROF" = true ]; then
    # PROFILING_FLAGS="$PROFILING_FLAGS -pg"
    echo "GProf is not working"
fi
if [ "$ENABLE_GPERFTOOLS" = true ] || [ "$ENABLE_PROF" = true ]; then
    PROFILING_FLAGS="$PROFILING_FLAGS -DENABLE_GPERFTOOLS=ON"
fi

# Build sanitizer flags and determine target binary
SANITIZER_FLAGS=""
TARGET_BIN="lemondb"

if [ "$ENABLE_ASAN" = true ]; then
    SANITIZER_FLAGS="-DENABLE_ASAN=ON"
    TARGET_BIN="lemondb_asan"
elif [ "$ENABLE_MSAN" = true ]; then
    SANITIZER_FLAGS="-DENABLE_MSAN=ON"
    TARGET_BIN="lemondb_msan"
elif [ "$ENABLE_TSAN" = true ]; then
    SANITIZER_FLAGS="-DENABLE_TSAN=ON"
    TARGET_BIN="lemondb_tsan"
elif [ "$ENABLE_UBSAN" = true ]; then
    SANITIZER_FLAGS="-DENABLE_UBSAN=ON"
    TARGET_BIN="lemondb_ubsan"
fi

if [[ $usr == "frank" ]]; then
  cmake -S . -B build -DCMAKE_CXX_COMPILER=/usr/lib/llvm18/bin/clang++ $SANITIZER_FLAGS -DCMAKE_EXPORT_COMPILE_COMMANDS=ON $PROFILING_FLAGS > tmp.log 2>&1
  cmake --build build -j$(nproc) > tmp.log 2>&1
  cp build/compile_commands.json .
  if [ "$VERBOSE" = true ]; then
      cat tmp.log
  fi
  rm tmp.log
elif [[ $usr == "114514" ]]; then # not working, replace with your own whoami
    cmake -S . -B build -DCMAKE_CXX_COMPILER=clang++-18 $SANITIZER_FLAGS $PROFILING_FLAGS > /dev/null 2>&1 # not working, replace with your own clang-18
    cmake --build build -j$(nproc) > /dev/null 2>&1
else
    cmake -S . -B build -DCMAKE_CXX_COMPILER=clang++-18 $SANITIZER_FLAGS $PROFILING_FLAGS > /dev/null 2>&1
    cmake --build build -j$(nproc)
fi

cp ./build/bin/$TARGET_BIN ./lemondb

# Pass profiling flags to test_correctness; --gprof kept as alias for gperftools
./test/test_correctness.sh --verbose=$VERBOSE --full=$FULL_COMPARE --valgrind=$ENABLE_VALGRIND --gprof=$ENABLE_GPROF --gperftools=$ENABLE_GPERFTOOLS --prof=$ENABLE_PROF

# Skip quality checks if profiling is enabled
if [ "$ENABLE_VALGRIND" = true ] || [ "$ENABLE_GPROF" = true ] || [ "$ENABLE_GPERFTOOLS" = true ] || [ "$ENABLE_PROF" = true ]; then
    exit 0
fi

if [ $usr == "frank" ]; then
    ./run_tidy.sh
else
    ./clangtidy.sh
    ./clangtidy_cleanup.sh
fi

cd test

./cpplint.sh > cpplint 2>&1
./cppcheck.sh > cppcheck 2>&1



