#!/bin/sh

dir=$1
dev=$2
output=$3

echo "" > $output

# filter
/usr/bin/time -v ./ts_bench_wisc $dir 2 0 0 1 35000 10000 500000000 400000 16 $dev 50 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_wisc $dir 2 0 0 1 35000 10000 500000000 400000 16 $dev 60 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_wisc $dir 2 0 0 1 35000 10000 500000000 400000 16 $dev 70 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_wisc $dir 2 0 0 1 35000 10000 500000000 400000 16 $dev 80 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_wisc $dir 2 0 0 1 35000 10000 500000000 400000 16 $dev 90 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_wisc $dir 2 0 0 1 35000 10000 500000000 400000 16 $dev 95 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_wisc $dir 2 0 0 1 35000 10000 500000000 400000 16 $dev 99 1>>$output 2>&1

/usr/bin/time -v ./ts_bench_wisc $dir 2 0 0 3 2000 10000 500000000 250000 16 $dev 0 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_wisc $dir 2 0 0 3 1000 10000 500000000 250000 16 $dev 0 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_wisc $dir 2 0 0 3 500 10000 500000000 250000 16 $dev 0 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_wisc $dir 2 0 0 3 200 10000 500000000 250000 16 $dev 0 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_wisc $dir 2 0 0 3 100 10000 500000000 250000 16 $dev 0 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_wisc $dir 2 0 0 3 50 10000 500000000 250000 16 $dev 0 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_wisc $dir 2 0 0 3 10 10000 500000000 250000 16 $dev 0 1>>$output 2>&1

/usr/bin/time -v ./ts_bench_wisc $dir 2 0 0 3 35000 10000 500000000 250000 16 0 $dev 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_wisc $dir 2 0 0 3 22000 10000 500000000 250000 16 0 $dev 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_wisc $dir 2 0 0 3 10000 10000 500000000 250000 16 0 $dev 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_wisc $dir 2 0 0 3 5000 10000 500000000 250000 16 0 $dev 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_wisc $dir 2 0 0 3 1000 10000 500000000 250000 16 0 $dev 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_wisc $dir 2 0 0 3 500 10000 500000000 250000 16 0 $dev 1>>$output 2>&1
/usr/bin/time -v ./ts_bench_wisc $dir 2 0 0 3 100 10000 500000000 250000 16 0 $dev 1>>$output 2>&1