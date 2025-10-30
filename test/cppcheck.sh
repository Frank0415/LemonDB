echo "=== Running cppcheck ==="

find ../src -name "*.h" -o -name "*.cpp" | xargs cppcheck --enable=all

echo "=== cppcheck complete ==="