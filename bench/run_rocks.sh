#!/bin/sh

val_size=$1
output=$2

echo "" > $output

./ycsb_rocks /mnt/pm983_block/eval_scan_1_16_$val_size/ 0 0 1 1 10 16000 200000 16 1000 1  1>>$output
./ycsb_rocks /mnt/pm983_block/eval_scan_1_16_$val_size/ 0 0 1 1 100 16000 200000 16 1000 1  1>>$output
./ycsb_rocks /mnt/pm983_block/eval_scan_1_16_$val_size/ 0 0 1 1 500 16000 200000 16 1000 1  1>>$output

./ycsb_rocks /mnt/pm983_block/eval_scan_1_16_$val_size/ 2 0 1 1 10 16000 200000 16 1000 1 1>>$output
./ycsb_rocks /mnt/pm983_block/eval_scan_1_16_$val_size/ 2 0 1 1 100 16000 200000 16 1000 1 1>>$output
./ycsb_rocks /mnt/pm983_block/eval_scan_1_16_$val_size/ 2 0 1 1 500 16000 200000 16 1000 1 1>>$output

./ycsb_rocks /mnt/pm983_block/eval_scan_1_16_$val_size/ 2 0 1024 1 10 16000 200000 16 1000 1 1>>$output
./ycsb_rocks /mnt/pm983_block/eval_scan_1_16_$val_size/ 2 0 1024 1 100 16000 200000 16 1000 1 1>>$output
./ycsb_rocks /mnt/pm983_block/eval_scan_1_16_$val_size/ 2 0 1024 1 500 16000 200000 16 1000 1 1>>$output