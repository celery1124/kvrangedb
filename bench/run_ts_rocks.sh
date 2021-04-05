#!/bin/sh

dev=$1
output=$2

echo "" > $output

# 
/usr/bin/time -v ./ts_bench_kv $dev 0 0 0 1 35000 10000 400000000 50000 16 50 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_kv $dev 0 0 0 1 35000 10000 400000000 50000 16 60 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_kv $dev 0 0 0 1 35000 10000 400000000 50000 16 70 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_kv $dev 0 0 0 1 35000 10000 400000000 50000 16 80 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_kv $dev 0 0 0 1 35000 10000 400000000 50000 16 90 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_kv $dev 0 0 0 1 35000 10000 400000000 50000 16 95 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_kv $dev 0 0 0 1 35000 10000 400000000 50000 16 99 1>>$output 2>&1

/usr/bin/time -v ./ts_bench_kv $dev 0 0 0 3 1000 10000 400000000 40000 16 50 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_kv $dev 0 0 0 3 500 10000 400000000 40000 16 50 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_kv $dev 0 0 0 3 200 10000 400000000 40000 16 50 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_kv $dev 0 0 0 3 100 10000 400000000 40000 16 50 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_kv $dev 0 0 0 3 50 10000 400000000 40000 16 50 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_kv $dev 0 0 0 3 10 10000 400000000 40000 16 50 1>>$output 2>&1

/usr/bin/time -v ./ts_bench_kv $dev 0 0 0 3 35000 10000 400000000 40000 16 50 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_kv $dev 0 0 0 3 22000 10000 400000000 40000 16 50 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_kv $dev 0 0 0 3 10000 10000 400000000 40000 16 50 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_kv $dev 0 0 0 3 5000 10000 400000000 40000 16 50 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_kv $dev 0 0 0 3 1000 10000 400000000 40000 16 50 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_kv $dev 0 0 0 3 500 10000 400000000 40000 16 50 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_kv $dev 0 0 0 3 100 10000 400000000 40000 16 50 1>>$output 2>&1
