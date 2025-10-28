#!/bin/bash

echo "=== Running clang-tidy ==="
files=$(find ../src -name "*.cpp" -o -name "*.h")
compile_commands_path="../build/compile_commands.json"

# Run clang-tidy on each file
for file in $files; do
  clang-tidy "$file" -p="$compile_commands_path" -checks='-*,clang-analyzer-*,-clang-analyzer-security*,-clang-analyzer-alpha*,bugprone-suspicious-string-compare,google-global-names-in-headers,misc-include-cleaner,misc-const-correctness,performance-*' \
  -warnings-as-errors='codequality-no-header-guard,cppcoreguidelines-init-variables,readability-redundant-*,performance' \
  --extra-arg=-D__clang_analyzer__ \
  --extra-arg-before=-std=c++20 \
  --extra-arg-before=-x --extra-arg-before=c++ \
  -header-filter='^(\.\./src/.*|.*\.h)$' \
  -- \
  -Wsystem-headers \
  codequality-*,modernize-*,portability-* 2>&1 | sed '/include\/c++/ {N;N;d;}' 
  # codequality-no-public-member-variables
  # misc-*
done

echo "=== clang-tidy complete ==="