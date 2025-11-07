#!/bin/bash

echo "=== Running clang-tidy ==="

usr=$(whoami)

cd ../src

files=$(find . -name "*.cpp" -o -name "*.h")


if [[ $usr == "frank" || $usr == "114514" ]]; then
  export TIDY="clang-tidy"
else
  export TIDY="clang-tidy-18"
fi

compile_commands_path="../build/compile_commands.json"

if [[ "$usr" == "frank" || "$usr" == "ve482" ]]; then
  declare -A check_groups=(
    ["bugprone"]='bugprone-*,-bugprone-easily-swappable-parameters,-clang-diagnostic-error,-bugprone-empty-catch'
    ["cppcoreguidelines"]='cppcoreguidelines-*,-clang-diagnostic-error,-cppcoreguidelines-pro-type-member-init'
    ["misc"]='misc-*,-clang-diagnostic-error'
    ["modernize"]='modernize-*,-modernize-use-trailing-return-type,-clang-diagnostic-error'
    ["performance"]='performance-*,-clang-diagnostic-error'
    ["portability"]='portability-*,-clang-diagnostic-error'
    ["readability"]='readability-*,-clang-diagnostic-error'
    ["google"]='google-*,-google-objc-function-naming,-clang-diagnostic-error'
  )

  for group in "${!check_groups[@]}"; do
    echo "Running $group checks..."
    for file in $files; do
      $TIDY "$file" -p="$compile_commands_path" \
      -checks="-*,${check_groups[$group]}" \
      --extra-arg=-D__clang_analyzer__ \
      --extra-arg-before=-std=c++20 \
      --extra-arg-before=-x --extra-arg-before=c++ \
      -header-filter="^(\.\./src/.*|.*\\.h)$" \
      -- \
      -Wsystem-headers 2>&1 | sed '/include\/c++/ {N;N;d;}' >> "clang.${group}.tidy"
    done
  done
else
  TIDY_CHECKS='-*,bugprone-*,cppcoreguidelines-*,misc-*,modernize-*,-modernize-use-trailing-return-type,performance-*,portability-*,readability-*,google-*,-clang-diagnostic-error,-cppcoreguidelines-pro-type-member-init,-google-objc-function-naming'

  for file in $files; do
    $TIDY "$file" -p="$compile_commands_path" \
    -checks="$TIDY_CHECKS" \
    --extra-arg=-D__clang_analyzer__ \
    --extra-arg-before=-std=c++20 \
    --extra-arg-before=-x --extra-arg-before=c++ \
    -header-filter="^(\.\./src/.*|.*\\.h)$" \
    -- \
    -Wsystem-headers 2>&1 | sed '/include\/c++/ {N;N;d;}'
  done
fi

echo "=== clang-tidy complete ==="