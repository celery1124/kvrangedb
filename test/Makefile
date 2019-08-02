
CC=g++
INCLUDES=-I../include  
SRC=../src
LIBS=-L../libs -lrt -pthread -lleveldb -std=c++11
CXXFLAG=-O2 -fPIC -w -march=native
CXXFLAG_DBG=-O0 -fPIC -w -march=native -g 

BTREE_SRCS=$(SRC)/kvbtree/bplustree.cc $(SRC)/kvbtree/cache.cc $(SRC)/kvbtree/hash.cc $(SRC)/kvbtree/write_batch.cc
SRCS=$(SRC)/kv_index_lsm.cc $(SRC)/kv_index_btree.cc $(SRC)/db_impl.cc ../util/comparator.cc $(BTREE_SRCS)

lsm:
	$(CC) db_test.cc -o db_test $(SRCS) $(INCLUDES) $(LIBS) $(CXXFLAG_DBG) -DKVINDEX_LSM

btree:
	$(CC) db_test.cc -o db_test $(SRCS) $(INCLUDES) $(LIBS) $(CXXFLAG_DBG) -DKVINDEX_BTREE

clean:
	rm -rf db_test
