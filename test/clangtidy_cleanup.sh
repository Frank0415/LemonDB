#!/bin/bash

echo "Cleaning up clang-tidy output files..."

for file in $(find . -name "clang.*.tidy"); do
    if [ -f "$file" ]; then
        echo "Cleaning $file"
        sed -i -e '/warnings generated\./d' \
               -e '/Suppressed.*warnings/d' \
               -e '/Use -header-filter=.* to display errors from all non-system headers/d' \
               -e '/Found compiler error(s)\./d' \
               -e '/Error while processing/d' \
               -e '/[0-9]* warnings and [0-9]* error.*generated\./d' \
               -e '/clang-diagnostic-error/{N;N;d;}' \
               "$file"
    fi
done

echo "Cleanup complete."
