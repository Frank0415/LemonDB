#!/bin/sh

pwd
echo "1"
cmake -S . -B build -DCMAKE_CXX_COMPILER=clang++
cmake --build build -j8
echo "2"
ls -la
echo "3"
echo "void"
echo "4"
ls test/data/queries
echo "5"

ls test/data
echo "6"

mkdir test/data/tmp
ls test/data/tmp
echo "7"
cd ..
ls -la
pwd
echo "8"
cd /
ls -la
pwd
echo "9"
