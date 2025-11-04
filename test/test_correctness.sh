#!/bin/bash

# Parse command-line arguments
VERBOSE=false
FULL_COMPARE=false

for arg in "$@"; do
    case $arg in
        --verbose=*)
            VERBOSE="${arg#*=}"
            ;;
        --full=*)
            FULL_COMPARE="${arg#*=}"
            ;;
        *)
            echo "Unknown option: $arg"
            exit 1
            ;;
    esac
done

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
if [ "$VERBOSE" = true ]; then
    echo "Running tests..."
    echo "================"
fi

cd test/data

# Ensure tmp directory exists
mkdir -p tmp

cp ../../build/bin/lemondb ./lemondb

for test in "${TESTS[@]}"; do
    if [ ! -f "queries/${test}.query" ]; then
        if [ "$VERBOSE" = true ]; then
            echo "SKIP: queries/${test}.query not found"
        fi
        continue
    fi
    
    if [ ! -f "stdout/${test}.out" ]; then
        if [ "$VERBOSE" = true ]; then
            echo "SKIP: stdout/${test}.out not found"
        fi
        continue
    fi
    
    # Clean tmp directory before each test
    rm -f tmp/*.tbl
    
    start=$(date +%s.%N)
    ./lemondb < "queries/${test}.query" > "1.out" 2>/dev/null
    end=$(date +%s.%N)
    elapsed=$(echo "$end - $start" | bc -l)
    elapsed_rounded=$(printf "%.4f" "$elapsed")

    if [ "$VERBOSE" = true ]; then
        echo "Test: ${test} completed in ${elapsed_rounded} seconds"
    else
        echo "${test}: ${elapsed_rounded} seconds"
    fi
    
    # Compare stdout output
    stdout_pass=true
    if diff -q "1.out" "stdout/${test}.out" > /dev/null 2>&1; then
        if [ "$VERBOSE" = true ]; then
            echo "PASS: ${test} stdout"
        fi
    else
        echo "FAIL: ${test} stdout"
        stdout_pass=false
        if [ "$VERBOSE" = true ]; then
            echo "Stdout differences found:"
            diff "1.out" "stdout/${test}.out" | head -20
        fi
    fi
    
    # Compare table files
    table_pass=true
    table_differences=""
    table_count=0
    
    # Compare all tmp files with corresponding dump files
    # The dump files are named like: ${test}_dump_*.tbl or ${test}_*_dump.tbl
    for tmp_file in tmp/*.tbl; do
        if [ -f "$tmp_file" ]; then
            base_name=$(basename "$tmp_file")
            # Expected dump file name: prepend test name to the tmp filename
            # tmp/dump_sTable0.tbl -> dump/single_read_dump_sTable0.tbl
            expected_dump="dump/${test}_${base_name}"
            
            ((table_count++))
            
            if [ -f "$expected_dump" ]; then
                # Quick compare: check line count and character count
                tmp_wc=$(wc "$tmp_file" | awk '{print $1, $3}')
                dump_wc=$(wc "$expected_dump" | awk '{print $1, $3}')
                
                if [ "$tmp_wc" != "$dump_wc" ]; then
                    table_pass=false
                    table_differences="${table_differences}\nLine/char count mismatch in ${test}_${base_name}:"
                    table_differences="${table_differences}\n  tmp:  $tmp_wc"
                    table_differences="${table_differences}\n  dump: $dump_wc"
                elif [ "$FULL_COMPARE" = true ]; then
                    # Full comparison: verify all lines match (independent of order)
                    python3 ../verify_tables.py "$tmp_file" "$expected_dump"
                    if [ $? -ne 0 ]; then
                        table_pass=false
                        table_differences="${table_differences}\nLine content mismatch in ${test}_${base_name}"
                    fi
                fi
            else
                table_pass=false
                table_differences="${table_differences}\nExpected dump file not found: ${test}_${base_name}"
            fi
        fi
    done
    
    # Check if we found any tables to compare
    if [ $table_count -eq 0 ]; then
        if [ "$VERBOSE" = true ]; then
            echo "INFO: ${test} - No table files to compare"
        fi
    fi
    
    if $table_pass; then
        if [ "$VERBOSE" = true ]; then
            echo "PASS: ${test} tables"
        fi
    else
        echo "FAIL: ${test} tables"
        if [ "$VERBOSE" = true ]; then
            echo -e "$table_differences"
        fi
    fi
    
    # Overall test result
    if $stdout_pass && $table_pass; then
        # Only output on pass if verbose mode (name + time)
        if [ "$VERBOSE" = true ]; then
            echo "PASS: ${test}"
        fi
        ((PASSED++))
    else
        # Always output failures with time
        echo "${test} ${elapsed_rounded} seconds"
        ((FAILED++))
    fi
    
    rm -f "1.out"
done

echo "================="
echo "Tests passed: ${PASSED}"
echo "Tests failed: ${FAILED}"

cd ..
