#!/bin/bash

VERBOSE=false

for arg in "$@"; do
    case $arg in
        --verbose=*)
            VERBOSE="${arg#*=}"
            ;;
    esac
done

line="WARNING: Files over 300 lines:"

if [ "$VERBOSE" = false ]; then
    line+=" (Use --verbose or -v to see files over 200 lines)"
fi

echo "$line"
find test src -name "*.cpp" -o -name "*.h" | xargs realpath | xargs wc -l | sort -nr | awk '$1 >= 300 && $2 != "total"'
if [ "$VERBOSE" = true ]; then
    echo "Files over 200 lines:"
    find test src -name "*.cpp" -o -name "*.h" | xargs realpath | xargs wc -l | sort -nr | awk '$1 >= 200 && $1 < 300 && $2 != "total"'
fi
