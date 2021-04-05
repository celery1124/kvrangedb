#!/bin/sh

dev=$1
num_keys=$2
hint=$3
output=$4

echo "" > $output


./ycsb_kv $dev 1 0 1 1 10 10000000 $hint 16 $num_keys 16 1>>$output
./ycsb_kv $dev 3 0 1 1 10 10000000 $hint 16 $num_keys 16 1>>$output

./ycsb_kv $dev 1 0 1024 1 10 10000000 $hint 16 $num_keys 16 1>>$output
./ycsb_kv $dev 3 0 1024 1 10 10000000 $hint 16 $num_keys 16 1>>$output
