# Bringing Order to the World : Range Queries for KV-SSD

This project aims at supporting range queries on KVSSD by storing a secondary ordered key index in device.

# Interface

Provide a leveldb like interface with:
1. Custom comparator
2. Batch update
3. Iterator for range query

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

Note: please keep the kvssd_emul.conf file in the executable file directory. This configuration file override the default configuration by disabling the iops model (run faster).

```bash
	export PRJ_HOME=$(pwd)
	cd $PRJ_HOME/test
	export LD_LIBRARY_PATH=$PRJ_HOME/libs/
	./db_test
```

# TODO

<del>Enhance building system</del>

Use environment variable to load different code configurations

LSM Tree:
1. merge code for range filter and value 
2. prefetch (currently only baseline)

B Tree:
1. implement delete
2. add concurrency control

