#!/bin/bash
echo "WARNING: Files over 300 lines:"
find test src -name "*.cpp" -o -name "*.h" | xargs realpath | xargs wc -l | sort -nr | awk '$1 >= 300 && $2 != "total"'
echo "Files over 200 lines:"
find test src -name "*.cpp" -o -name "*.h" | xargs realpath | xargs wc -l | sort -nr | awk '$1 >= 200 && $1 < 300 && $2 != "total"'

usr=$(whoami)

if [[ $usr == "frank" ]]; then
  cmake -S . -B build -DCMAKE_CXX_COMPILER=/usr/lib/llvm18/bin/clang++ -DENABLE_ASAN=ON -DENABLE_MSAN=ON -DENABLE_UBSAN=ON
  cmake --build build -j$(nproc)
elif [[ $usr == "114514" ]]; then # not working, replace with your own whoami
    cmake -S . -B build -DCMAKE_CXX_COMPILER=clang++-18 -DENABLE_ASAN=ON -DENABLE_MSAN=ON -DENABLE_UBSAN=ON > /dev/null 2>&1 # not working, replace with your own clang-18
    cmake --build build -j$(nproc) > /dev/null 2>&1
else
    cmake -S . -B build -DCMAKE_CXX_COMPILER=clang++-18 > /dev/null 2>&1
    cmake --build build -j$(nproc) > /dev/null 2>&1
fi

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
    
    start=$(date +%s.%N)
    ./lemondb < "queries/${test}.query" > "1.out" 2>/dev/null
    end=$(date +%s.%N)
    elapsed=$(echo "$end - $start" | bc)

    echo "Test: ${test} completed in ${elapsed} seconds"
    cnt=$(wc -l < "1.out")
    # Compare output
    if diff -q "1.out" "stdout/${test}.out" > /dev/null 2>&1; then
        echo "PASS: ${test}"
        ((PASSED++))
    else
        echo "FAIL: ${test}"
        ((FAILED++))
        echo "Differences found:"
        diff "1.out" "stdout/${test}.out" | head -50
    fi
    rm -f "1.out"
done

echo "================="
echo "Tests passed: ${PASSED}"
echo "Tests failed: ${FAILED}"

cd ..

./clangtidy.sh
./cpplint.sh
./cppcheck.sh
./clangtidy_cleanup.sh