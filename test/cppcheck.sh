echo "=== Running cppcheck ==="

find ../src -name "*.h" -o -name "*.cpp" | xargs cppcheck --enable=all 2>&1

echo "=== cppcheck complete ==="