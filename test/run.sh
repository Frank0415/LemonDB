#!/bin/sh

echo "4"
ls test/data/queries
echo "5"

ls test/data
echo "6"

mkdir test/data/tmp
ls test/data/tmp
echo "7"
cd /
ls -la
pwd
echo "8"
cd /usr
ls -la
pwd
echo "9"
cd test
touch run.sh
echo "#!/bin/bash

cd ..
cmake -S . -B build -DCMAKE_CXX_COMPILER=clang++ -DENABLE_ASAN=ON -DENABLE_MSAN=ON -DENABLE_UBSAN=ON
cmake --build build -j8
cd test

# Copy the binary to test directory
# cp ../build/bin/lemondb_asan ./lemondb
# cp ../build/bin/lemondb_msan ./lemondb
cp ../build/bin/lemondb_ubsan ./lemondb
# cp ../build/bin/lemondb ./lemondb

# Test files
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
  if [ ! -f "queries/${test}.query" ]; then
    echo "⚠ SKIP: queries/${test}.query not found"
    continue
  fi

  if [ ! -f "stdout/${test}.out" ]; then
    echo "⚠ SKIP: stdout/${test}.out not found"
    continue
  fi

  # Run test
#   head -40 "queries/${test}.query" | ./lemondb >1.out 2>&1
  ./lemondb < "queries/${test}.query" > 1.out 2>/dev/null
  # Compare output
  if diff -q 1.out "stdout/${test}.out" >/dev/null 2>&1; then
    echo "✓ PASS: ${test}"
    ((PASSED++))
  else
    echo "✗ FAIL: ${test}"
    ((FAILED++))
    # Show first 10 lines of diff
    echo "  First differences:"
    diff 1.out "stdout/${test}.out" | head -20 | sed 's/^/    /'
  fi
done

echo "================"
echo "Results: $PASSED passed, $FAILED failed"

# Clean up
# rm -f 1.out

if [ $FAILED -eq 0 ]; then
  exit 0
else
  exit 1
fi
" >> run.sh
chmod +x run.sh
./run.sh