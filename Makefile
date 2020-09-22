# (A) Optimized mode
 OPT ?= -O2 -DNDEBUG
# (B) Debug mode
# OPT ?= -g -O0

TARGET=libkvrangedb.so

HOME=$(shell pwd)
CC=gcc
INCLUDES=-I$(HOME)/include
LIBS=-L$(HOME)/libs -Wl,-rpath,$(HOME)/libs -lrt -lpthread -lleveldb -lrocksdb -lkvssd -ltbb
CXXFLAG=-fPIC -w -march=native -std=c++11 $(OPT)

DB_SRCS=$(HOME)/src/kv_index_lsm.cc $(HOME)/src/kv_index_rocks.cc $(HOME)/src/kv_index_btree.cc $(HOME)/src/kv_index_base.cc $(HOME)/src/kv_index_inmem.cc $(HOME)/src/db_impl.cc $(HOME)/src/db_iter.cc $(HOME)/src/hash.cc
KVBTREE_SRCS=$(HOME)/src/kvbtree/bplustree.cc $(HOME)/src/kvbtree/cache.cc $(HOME)/src/kvbtree/hash.cc $(HOME)/src/kvbtree/write_batch.cc
BASE_SRCS=$(HOME)/src/base/base.cc
INMEM_SRCS=$(HOME)/src/inmem/inmem.cc
KVSSD_SRCS=$(HOME)/src/kvssd/kvssd.cc
UTIL_SRCS=$(HOME)/util/comparator.cc
SRCS=$(DB_SRCS) $(KVBTREE_SRCS) $(BASE_SRCS) $(INMEM_SRCS) $(UTIL_SRCS)

all: kvssd leveldb rocksdb kvrangedb

kvssd:
	$(CC) -shared -o $(HOME)/libs/libkvssd.so $(INCLUDES) $(KVSSD_SRCS) -L$(HOME)/libs -Wl,-rpath,$(HOME)/libs -lkvapi -lnuma $(CXXFLAG)

rocksdb:
	make -C $(HOME)/src/rocksdb/ shared_lib -j8
	cp $(HOME)/src/rocksdb/librocksdb* $(HOME)/libs

leveldb:
	make -C $(HOME)/src/leveldb/
	cp $(HOME)/src/leveldb/out-shared/libleveldb.so* $(HOME)/libs

kvrangedb:
	$(CC) -shared -o $(HOME)/libs/$(TARGET) $(SRCS) $(INCLUDES) $(LIBS) $(CXXFLAG)

clean:
	make -C $(HOME)/src/leveldb/ clean
	make -C $(HOME)/src/rocksdb/ clean
	rm -rf $(HOME)/libs/$(TARGET) $(HOME)/libs/libkvssd.so $(HOME)/libs/libleveldb.so* $(HOME)/libs/librocksdb*
