#!/bin/sh

cmake -S . -B build -DCMAKE_CXX_COMPILER=clang -DCMAKE_C_COMPILER=clang
cmake --build build -j2
echo "4"
ls test/data/queries
echo "5"

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

for test in "${TESTS[@]}"; do
    if [ ! -f "test/data/queries/${test}.query" ]; then
        echo "SKIP: test/data/queries/${test}.query not found"
        continue
    fi
    
    if [ ! -f "test/data/stdout/${test}.out" ]; then
        echo "SKIP: test/data/stdout/${test}.out not found"
        continue
    fi
    
    # Run test
    ./build/bin/lemondb < "test/data/queries/${test}.query" > 1.out 2>/dev/null
    # Compare output
    if diff -q 1.out "test/data/stdout/${test}.out" >/dev/null 2>&1; then
        echo "PASS: ${test}"
        ((PASSED++))
    else
        echo "FAIL: ${test}"
        ((FAILED++))
        diff 1.out "test/data/stdout/${test}.out"
    fi
done

echo "================="
echo "Tests passed: ${PASSED}"
echo "Tests failed: ${FAILED}"

ls test/data/tmp

for file in test/data/tmp/*; do
    cat $file
done