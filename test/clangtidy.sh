#!/bin/bash

echo "=== Running clang-tidy ==="

usr=$(whoami)

files=$(find ../src -name "*.cpp" -o -name "*.h")


if [[ $usr == "frank" || $usr == "114514" ]]; then
  export TIDY="clang-tidy"
else
  export TIDY="clang-tidy-18"
fi

compile_commands_path="../build/compile_commands.json"

if [[ "$usr" == "frank" || "$usr" == "ve482" ]]; then
  declare -A check_groups=(
    ["bugprone"]='bugprone-*'
    ["cppcoreguidelines"]='cppcoreguidelines-*'
    ["misc"]='misc-*'
    ["modernize"]='modernize-*,-modernize-use-trailing-return-type'
    ["performance"]='performance-*'
    ["portability"]='portability-*'
    ["readability"]='readability-*'
    ["google"]='google-*'
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
  TIDY_CHECKS='-*,bugprone-*,cppcoreguidelines-*,misc-*,modernize-*,-modernize-use-trailing-return-type,performance-*,portability-*,readability-*,google-*'

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