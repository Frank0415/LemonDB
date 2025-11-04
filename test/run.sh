#!/bin/bash

# Parse command line arguments
FULL_COMPARE=false
VERBOSE=false

for arg in "$@"; do
    case "$arg" in
        --full|-f)
            FULL_COMPARE=true
            ;;
        --verbose|-v)
            VERBOSE=true
            ;;
    esac
done

if [ "$VERBOSE" = true ]; then
    echo "Verbose mode enabled"
    [ "$FULL_COMPARE" = true ] && echo "Full comparison mode enabled"
    echo ""
fi

./test/linecount.sh --verbose=$VERBOSE

usr=$(whoami)

if [[ $usr == "frank" ]]; then
  cmake -S . -B build -DCMAKE_CXX_COMPILER=/usr/lib/llvm18/bin/clang++ -DENABLE_ASAN=ON -DENABLE_MSAN=ON -DENABLE_UBSAN=ON > tmp.log 2>&1
  cmake --build build -j$(nproc) > tmp.log 2>&1
  if [ "$VERBOSE" = true ]; then
      cat tmp.log
  fi
elif [[ $usr == "114514" ]]; then # not working, replace with your own whoami
    cmake -S . -B build -DCMAKE_CXX_COMPILER=clang++-18 -DENABLE_ASAN=ON -DENABLE_MSAN=ON -DENABLE_UBSAN=ON > /dev/null 2>&1 # not working, replace with your own clang-18
    cmake --build build -j$(nproc) > /dev/null 2>&1
else
    cmake -S . -B build -DCMAKE_CXX_COMPILER=clang++-18 > /dev/null 2>&1
    cmake --build build -j$(nproc) > /dev/null 2>&1
fi

cp ./build/bin/lemondb ./lemondb

./test/test_correctness.sh --verbose=$VERBOSE --full=$FULL_COMPARE

if [ $usr == "frank" ]; then
    ./run_tidy.sh
fi

cd test

./cpplint.sh > cpplint 2>&1
./cppcheck.sh > cppcheck 2>&1
./clangtidy.sh
./clangtidy_cleanup.sh


