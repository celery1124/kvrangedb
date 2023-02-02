# Bringing Order to the World : Range Queries for KV-SSD

This project aims at supporting range queries on KVSSD by storing a secondary ordered key index in device.

We compare with an in-house [WiscKey][wisckey repo] implementation on block SSD and [RocksKV][rockskv repo], a simple RocksDB porting to KVSSD.

# Publication
Mian Qin, Qing Zheng, Jason Lee, Bradley Settlemyer, Fei Wen, Narasimha Reddy, Paul Gratz, [KVRangeDB: Range Queries for A Hash-Based Key-Value Device](https://drive.google.com/file/d/1iDKdrF8HWRRjsbWnnm-QK2x9UMxddEoD/view?usp=share_link), ACM Transactions on Storage, Vol. 1, No. 1, article to appear Jan. 2023. [https://dl.acm.org/doi/10.1145/3582013](https://dl.acm.org/doi/10.1145/3582013)

# Interface

Provide a rocksdb like interface with:
1. Custom comparator
2. Batch update
3. Iterator for range query (**currently only support forward iterator**)

# YCSB binding

A simple java native interface (JNI) implementation with YCSB client is created for KVRangeDB. Please refer to the repo [ycsb-bindings][ycsb-bindings repo].

# Build Test example

## build Samsung KVSSD

For more details, please refer to KVSSD_QUICK_START_GUIDE.pdf by Samsung (under KVSSD root directory).

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

### build with real device

```bash
        # build kvssd device driver
        cd $PRJ_HOME/KVSSD-1.2.0/PDK/driver/PCIe/kernel_driver/kernel_v<version>/
        make clean
        make all
        sudo ./re_insmod

        # build kvapi library
        export PRJ_HOME=$(pwd)
        export KVSSD_HOME=$PRJ_HOME/KVSSD-1.2.0/PDK/core
        $KVSSD_HOME/tools/install_deps.sh # install kvapi dependency
        mkdir $KVSSD_HOME/build
        cd $KVSSD_HOME/build
        cmake -DWITH_KDD=ON $KVSSD_HOME
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
[ycsb-bindings repo]:https://github.com/celery1124/ycsb-bindings
