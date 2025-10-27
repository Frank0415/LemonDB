#!/bin/bash

echo "WARNING: Files over 300 lines:"
find src -type f -print0 | xargs -0 wc -l | sort -nr | awk '$1 >= 300 && $2 != "total"'

echo "Files over 200 lines:"
find src -type f -print0 | xargs -0 wc -l | sort -nr | awk '$1 >= 200 && $1 < 300 && $2 != "total"'

cmake -S . -B build -DCMAKE_CXX_COMPILER=clang++-18 > /dev/null 2>&1
cmake --build build -j$(nproc) > /dev/null 2>&1

cp ./build/bin/lemondb ./lemondb

TESTS=(
  "single_read"
  "single_read_dup"
  "single_read_update"
  "single_insert_delete"
  "few_read"
  "few_read_dup"
  "few_read_update"
  "few_insert_delete"
  "many_read"
  "many_read_dup"
  "many_read_update"
  "many_insert_delete"
  "test"
)

PASSED=0
FAILED=0
echo "Running tests..."
echo "================"

cd test/data

cp ../../build/bin/lemondb ./lemondb

for test in "${TESTS[@]}"; do
    if [ ! -f "queries/${test}.query" ]; then
        echo "SKIP: queries/${test}.query not found"
        continue
    fi
    
    if [ ! -f "stdout/${test}.out" ]; then
        echo "SKIP: stdout/${test}.out not found"
        continue
    fi
    
    # Run test
    ./lemondb < "queries/${test}.query" > 1.out 2>/dev/null
    # Compare output
    if diff -q 1.out "stdout/${test}.out" >/dev/null 2>&1; then
        echo "PASS: ${test}"
        ((PASSED++))
    else
        echo "FAIL: ${test}"
        ((FAILED++))
        # diff 1.out "test/data/stdout/${test}.out"
    fi
done

rm 1.out

echo "================="
echo "Tests passed: ${PASSED}"
echo "Tests failed: ${FAILED}"
