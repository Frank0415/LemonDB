#!/bin/bash

# Valgrind/Callgrind profiling script
# Runs each test with Valgrind Callgrind and generates separate profile files

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

# Change to test/data directory
cd test/data || exit 1

echo "Starting Valgrind profiling for all tests..."

for test in "${TESTS[@]}"; do
    if [ -f "queries/${test}.query" ]; then
        echo "Profiling test: ${test}"
        # Run with Valgrind Callgrind, suppress output
        valgrind --tool=callgrind --dump-instr=yes --callgrind-out-file=${test}.callgrind.out ./lemondb < "queries/${test}.query" > /dev/null 2>&1
        echo "Generated: ${test}.callgrind.out"
    else
        echo "Skipping ${test}: query file not found"
    fi
done

cd .. 
mkdir -p prof.callgrind

cd data

mv *.callgrind.out ../prof.callgrind

cd ../..

echo "Valgrind profiling completed."
echo "Use 'kcachegrind ${test}.callgrind.out' to analyze each test."