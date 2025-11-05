#!/bin/bash

# gperftools profiling script
# Profiles each test using Google gperftools CPU profiler (sampling).

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

# Ensure pprof and libprofiler are available
PPROF_BIN=$(command -v pprof || true)
LIBPROF_CAND="/usr/lib/libprofiler.so /usr/lib/libprofiler.so.0 /usr/lib64/libprofiler.so /usr/lib64/libprofiler.so.0"
LIBPROF_PATH=""
for p in $LIBPROF_CAND; do
    if [ -f "$p" ]; then
        LIBPROF_PATH=$p
        break
    fi
done

# Work in test/data
cd test/data || exit 1

mkdir -p profiles

echo "Starting gperftools profiling for all tests..."

for test in "${TESTS[@]}"; do
    if [ -f "queries/${test}.query" ]; then
        echo "Profiling test: ${test}"
        PROFILE_FILE="profiles/${test}.prof"

        if [ -n "$LIBPROF_PATH" ]; then
            # Use LD_PRELOAD + CPUPROFILE to collect profile without recompilation
            env -u CPUPROFILE LD_PRELOAD="$LIBPROF_PATH" CPUPROFILE="$PROFILE_FILE" ../../lemondb < "queries/${test}.query" > /dev/null 2>&1
        else
            # Try without LD_PRELOAD; this will only work if lemondb was linked with -lprofiler
            ../../lemondb < "queries/${test}.query" > /dev/null 2>&1
        fi

        if [ -f "$PROFILE_FILE" ]; then
            if [ -n "$PPROF_BIN" ]; then
                # Convert to callgrind-like format for KCachegrind: pprof --callgrind
                $PPROF_BIN --callgrind ../../lemondb "$PROFILE_FILE" > "${test}.callgrind.out" 2>/dev/null || true
                echo "Generated: ${test}.callgrind.out (from ${PROFILE_FILE})"
            else
                echo "Profile collected: ${PROFILE_FILE} (pprof not found; install gperftools to convert/analyze)"
            fi
        else
            echo "No profile produced for ${test}."
            echo "If you want to profile without recompilation, install gperftools and ensure libprofiler is at a standard path so this script can LD_PRELOAD it." 
            echo "Alternatively, link your binary with -lprofiler and re-run to get a profile file named '${test}.prof'."
        fi
    else
        echo "Skipping ${test}: query file not found"
    fi
done

echo "gperftools profiling completed."
echo "Use 'kcachegrind <test>.callgrind.out' to analyze each test."
