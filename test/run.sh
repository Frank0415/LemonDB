#!/bin/bash

# Parse command line arguments
FULL_COMPARE=false
VERBOSE=false
ENABLE_VALGRIND=false
ENABLE_GPROF=false
ENABLE_PROF=false

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
            ENABLE_GPROF=true
            ;;
        --prof)
            ENABLE_PROF=true
            ;;
        --help|-h)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --full, -f          Enable full comparison mode"
            echo "  --verbose, -v       Enable verbose output"
            echo "  --valgrind          Enable Valgrind/Callgrind profiling"
            echo "  --gprof             Enable GProf profiling"
            echo "  --prof              Enable both Valgrind and GProf profiling"
            echo "  --help, -h          Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0 --verbose --full"
            echo "  $0 --valgrind"
            echo "  $0 --gprof"
            echo "  $0 --prof"
            exit 0
            ;;
    esac
done

if [ "$VERBOSE" = true ]; then
    echo "Verbose mode enabled"
    [ "$FULL_COMPARE" = true ] && echo "Full comparison mode enabled"
    [ "$ENABLE_VALGRIND" = true ] && echo "Valgrind profiling enabled"
    [ "$ENABLE_GPROF" = true ] && echo "GProf profiling enabled"
    [ "$ENABLE_PROF" = true ] && echo "Both profiling enabled"
    echo ""
fi

if [ "$ENABLE_VALGRIND" != true ] && [ "$ENABLE_GPROF" != true ] && [ "$ENABLE_PROF" != true ]; then
    ./test/linecount.sh --verbose=$VERBOSE
fi

usr=$(whoami)

# Build profiling flags
PROFILING_FLAGS=""
if [ "$ENABLE_VALGRIND" = true ] || [ "$ENABLE_PROF" = true ]; then
    PROFILING_FLAGS="$PROFILING_FLAGS -DENABLE_CALLGRIND=ON -DCMAKE_BUILD_TYPE=Debug"
fi
if [ "$ENABLE_GPROF" = true ] || [ "$ENABLE_PROF" = true ]; then
    PROFILING_FLAGS="$PROFILING_FLAGS -DENABLE_GPROF=ON"
fi

if [[ $usr == "frank" ]]; then
  cmake -S . -B build -DCMAKE_CXX_COMPILER=/usr/lib/llvm18/bin/clang++ -DENABLE_ASAN=ON -DENABLE_MSAN=ON -DENABLE_UBSAN=ON -DCMAKE_EXPORT_COMPILE_COMMANDS=ON $PROFILING_FLAGS > tmp.log 2>&1
  cmake --build build -j$(nproc) > tmp.log 2>&1
  cp build/compile_commands.json .
  if [ "$VERBOSE" = true ]; then
      cat tmp.log
  fi
  rm tmp.log
elif [[ $usr == "114514" ]]; then # not working, replace with your own whoami
    cmake -S . -B build -DCMAKE_CXX_COMPILER=clang++-18 -DENABLE_ASAN=ON -DENABLE_MSAN=ON -DENABLE_UBSAN=ON $PROFILING_FLAGS > /dev/null 2>&1 # not working, replace with your own clang-18
    cmake --build build -j$(nproc) > /dev/null 2>&1
else
    cmake -S . -B build -DCMAKE_CXX_COMPILER=clang++-18 $PROFILING_FLAGS > /dev/null 2>&1
    cmake --build build -j$(nproc) > /dev/null 2>&1
fi

cp ./build/bin/lemondb ./lemondb

./test/test_correctness.sh --verbose=$VERBOSE --full=$FULL_COMPARE --valgrind=$ENABLE_VALGRIND --gprof=$ENABLE_GPROF --prof=$ENABLE_PROF

# Skip quality checks if profiling is enabled
if [ "$ENABLE_VALGRIND" = true ] || [ "$ENABLE_GPROF" = true ] || [ "$ENABLE_PROF" = true ]; then
    exit 0
fi

if [ $usr == "frank" ]; then
    ./run_tidy.sh
fi

cd test

./cpplint.sh > cpplint 2>&1
./cppcheck.sh > cppcheck 2>&1
./clangtidy.sh
./clangtidy_cleanup.sh


