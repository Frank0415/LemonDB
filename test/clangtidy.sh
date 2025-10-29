#!/bin/bash

echo "=== Running clang-tidy ==="

usr=$(whoami)

files=$(find ../src ../test -name "*.cpp" -o -name "*.h")


if [[ $usr == "frank" || $usr == "114514" ]]; then
  export TIDY="clang-tidy"
else 
  export TIDY="clang-tidy-18"
fi

compile_commands_path="../build/compile_commands.json"

TIDY_CHECKS='-*,bugprone-*,cppcoreguidelines-*,misc-*,modernize-*,performance-*,portability-*,readability-*,google-*'

for file in $files; do
  $TIDY "$file" -p="$compile_commands_path" \
  -checks="$TIDY_CHECKS" \
  --extra-arg=-D__clang_analyzer__ \
  --extra-arg-before=-std=c++20 \
  --extra-arg-before=-x --extra-arg-before=c++ \
  -header-filter='^(\.\./src/.*|.*\.h)$' \
  -- \
  -Wsystem-headers 2>&1 | sed '/include\/c++/ {N;N;d;}'
done

echo "=== clang-tidy complete ==="