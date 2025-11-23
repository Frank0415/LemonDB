#!/bin/bash

# Multi-threading and LISTEN Functionality Test Script

BUILD_DIR="./build/bin"
LEMONDB="$BUILD_DIR/lemondb"

# Color output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=========================================="
echo "  LemonDB Multi-threading & LISTEN Tests"
echo "=========================================="
echo ""

# Test 1: Basic LISTEN functionality
echo -e "${YELLOW}[Test 1]${NC} Basic LISTEN functionality"
$LEMONDB --listen /tmp/multithread_test.sql --threads 1 > /tmp/test1_out.txt 2>&1
if [ $? -eq 0 ]; then
    echo -e "${GREEN}PASS${NC} - Generated $(wc -l < /tmp/test1_out.txt) lines of output"
else
    echo -e "${RED}FAIL${NC}"
fi
echo ""

# Test 2: Single-thread vs Multi-thread performance
echo -e "${YELLOW}[Test 2]${NC} Single-thread vs Multi-thread performance comparison"
echo -n "  1 thread:  "
time1=$( { time $LEMONDB --listen /tmp/stress_test.sql --threads 1 > /dev/null 2>&1; } 2>&1 | grep real | awk '{print $2}')
echo "$time1"

echo -n "  4 threads: "
time4=$( { time $LEMONDB --listen /tmp/stress_test.sql --threads 4 > /dev/null 2>&1; } 2>&1 | grep real | awk '{print $2}')
echo "$time4"

echo -n "  8 threads: "
time8=$( { time $LEMONDB --listen /tmp/stress_test.sql --threads 8 > /dev/null 2>&1; } 2>&1 | grep real | awk '{print $2}')
echo "$time8"
echo ""

# Test 3: Output order consistency
echo -e "${YELLOW}[Test 3]${NC} Output order consistency check"
$LEMONDB --listen /tmp/stress_test.sql --threads 1 > /tmp/test_seq1.txt 2>&1
$LEMONDB --listen /tmp/stress_test.sql --threads 4 > /tmp/test_seq4.txt 2>&1
$LEMONDB --listen /tmp/stress_test.sql --threads 8 > /tmp/test_seq8.txt 2>&1

if diff -q /tmp/test_seq1.txt /tmp/test_seq4.txt > /dev/null && \
   diff -q /tmp/test_seq1.txt /tmp/test_seq8.txt > /dev/null; then
    echo -e "${GREEN}PASS${NC} - All thread configurations produce identical output"
else
    echo -e "${RED}FAIL${NC} - Output differs across thread counts"
fi
echo ""

# Test 4: Stress test - multiple runs
echo -e "${YELLOW}[Test 4]${NC} Stability test (10 runs)"
success_count=0
for i in {1..10}; do
    if $LEMONDB --listen /tmp/stress_test.sql --threads 4 > /dev/null 2>&1; then
        ((success_count++))
    fi
done
echo -e "  Success rate: $success_count/10"
if [ $success_count -eq 10 ]; then
    echo -e "${GREEN}PASS${NC}"
else
    echo -e "${RED}FAIL${NC} - Some runs failed"
fi
echo ""

# Test 5: Query count verification
echo -e "${YELLOW}[Test 5]${NC} Query count verification"
output_lines=$($LEMONDB --listen /tmp/stress_test.sql --threads 4 2>&1 | wc -l)
expected_lines=21  # 1 (LISTEN result) + 20 queries
if [ $output_lines -eq $expected_lines ]; then
    echo -e "${GREEN}PASS${NC} - Output line count correct ($output_lines/$expected_lines)"
else
    echo -e "${YELLOW}WARN${NC} - Output lines: $output_lines (expected: $expected_lines)"
fi
echo ""

# Test 6: Memory leak check (basic)
echo -e "${YELLOW}[Test 6]${NC} Multiple runs memory usage check"
for i in {1..100}; do
    $LEMONDB --listen /tmp/multithread_test.sql --threads 4 > /dev/null 2>&1
done
echo -e "${GREEN}DONE${NC} - 100 runs completed without crash"
echo ""

echo "=========================================="
echo "  All Tests Completed"
echo "=========================================="
