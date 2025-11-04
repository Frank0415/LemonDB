#!/bin/bash

echo "WARNING: Files over 300 lines:"
find test src -name "*.cpp" -o -name "*.h" | xargs realpath | xargs wc -l | sort -nr | awk '$1 >= 300 && $2 != "total"'
echo "Files over 200 lines:"
find test src -name "*.cpp" -o -name "*.h" | xargs realpath | xargs wc -l | sort -nr | awk '$1 >= 200 && $1 < 300 && $2 != "total"'