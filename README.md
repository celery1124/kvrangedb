# Bringing Order to the World : Range Queries for KV-SSD

This project aims at supporting range queries on KVSSD by storing a secondary ordered key index in device.

# Interface

Provide a leveldb like interface with:
1. Custom comparator
2. Batch update
3. Iterator for range query

# Build Test example

## build

```bash
	export PRJ_HOME=./
	cd $PRJ_HOME/leveldb
	make
	cp out-shared/libleveldb.so* $PRJ_HOME/libs/
	cd $PRJ_HOME/test
	make lsm # lsm index
	make btree # btree index
```

## run
```bash
	./db_test
```

# TODO

Enhance building system

LSM Tree:
1. merge code for range filter and value 
2. prefetch (currently only baseline)

B Tree:
1. implement delete
2. add concurrency control

