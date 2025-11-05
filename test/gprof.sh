#!/bin/bash

# GProf profiling script
# Runs each test and generates GProf analysis

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

echo "Starting GProf profiling for all tests..."

for test in "${TESTS[@]}"; do
    if [ -f "queries/${test}.query" ]; then
        echo "Profiling test: ${test}"
        # Run the test to generate gmon.out
        ../../lemondb < "queries/${test}.query" > /dev/null 2>&1
        # Rename gmon.out to test-specific name
        if [ -f "gmon.out" ]; then
            mv gmon.out ${test}.gmon.out
            # Generate GProf analysis
            gprof ../../lemondb ${test}.gmon.out > ${test}.gprof.txt
            echo "Generated: ${test}.gprof.txt"
        fi
    else
        echo "Skipping ${test}: query file not found"
    fi
done

echo "GProf profiling completed."
echo "Check test/prof.gprof/${test}.gprof.txt files for analysis."

cd ..
mkdir -p prof.gprof
cd data
mv *.gprof.txt ../prof.gprof 2>/dev/null || true
cd ../..

echo "Profile files moved to test/prof.gprof/"