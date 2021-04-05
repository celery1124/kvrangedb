# Bringing Order to the World : Range Queries for KV-SSD

This project aims at supporting range queries on KVSSD by storing a secondary ordered key index in device.

We compare with an in-house [WiscKey][wisckey repo] implementation on block SSD and [RocksKV][rockskv repo], a simple RocksDB porting to KVSSD.

# Interface

Provide a rocksdb like interface with:
1. Custom comparator
2. Batch update
3. Iterator for range query (**currently only support forward iterator**)

# Build Test example

## build Samsung KVSSD

### build emulator (environment without actual device)

```bash
	# build kvapi library
	export PRJ_HOME=$(pwd)
	export KVSSD_HOME=$PRJ_HOME/KVSSD-1.2.0/PDK/core
	$KVSSD_HOME/tools/install_deps.sh # install kvapi dependency
	mkdir $KVSSD_HOME/build
	cd $KVSSD_HOME/build
	cmake -DWITH_EMU=ON $KVSSD_HOME
	make -j4

	# copy libkvapi.so
	mkdir $PRJ_HOME/libs
	cp $KVSSD_HOME/build/libkvapi.so $PRJ_HOME/libs/
```

## build kvrangedb library

```bash
	make
```

## build test case

```bash
	export PRJ_HOME=$(pwd)
	cd $PRJ_HOME/test
	make lsm # lsm index
	make btree # btree index
```

## run

Configure DB options by environment: (please refer to include/kvrangedb/options.h)
```bash
	export INDEX_TYPE=LSM # LSM, BTREE, BASE
	export PREFETCH_ENA=TRUE # TRUE, FALSE
	export PREFETCH_PREFETCH_DEPTH=16 # any integer
	export RANGE_FILTER_ENA=TRUE # TRUE, FALSE
```

Note: please keep the kvssd_emul.conf file in the executable file directory. This configuration file override the default configuration by disabling the iops model (run faster).

```bash
	export PRJ_HOME=$(pwd)
	cd $PRJ_HOME/test
	export LD_LIBRARY_PATH=$PRJ_HOME/libs/
	./db_test
```

# TODO

<del>Enhance building system</del>

<del>Use environment variable to load different code configurations</del>

<del>Optimize value prefetch (when to prefetch, seperate thread for issuing I/O)</del>

<del>LSM Tree:</del>
1. <del>merge code for range filter and</del>
2. <del>value prefetch (currently only baseline)</del>



[wisckey repo]:https://github.com/celery1124/wisckey
[rockskv repo]:https://github.com/celery1124/rockskv