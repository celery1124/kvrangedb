CC=g++
INCLUDES=-I../include
LIBS=-L../libs -lkvrangedb -lkvapi
CXXFLAG=-O2 -fPIC -w -march=native -std=c++11 -pthread
CXXFLAG_DBG=-O0 -fPIC -w -march=native -g -std=c++11 -pthread

ROCKS_INCLUDE=-I./include
ROCKS_LIB=librocksdb.a -lbz2 -lsnappy -lz

WISC_INCLUDE=-I./include
WISC_LIB=-L/home/grads/c/celery1124/wisckey/libs -lwisckey

all: rocks kv

rocks:
	$(CC) ts_bench_rocks.cc $(ROCKS_LIB) -o ts_bench_rocks $(ROCKS_INCLUDE) $(CXXFLAG)

kv:
	$(CC) ts_bench_kv.cc -o ts_bench_kv $(INCLUDES) $(LIBS) $(CXXFLAG)

wisckey:
	$(CC) ts_bench_wisc.cc -o ts_bench_wisc $(WISC_INCLUDE) $(WISC_LIB) $(CXXFLAG)

ycsb_kv:
	$(CC) ycsb_kv.cc -o ycsb_kv $(INCLUDES) $(LIBS) $(CXXFLAG)

ycsb_wisckey:
	$(CC) ycsb_wisc.cc -o ycsb_wisc $(WISC_INCLUDE) $(WISC_LIB) $(CXXFLAG)

ycsb_rocks:
	$(CC) ycsb_rocks.cc $(ROCKS_LIB) -o ycsb_rocks $(ROCKS_INCLUDE) $(CXXFLAG)

clean:
	rm -rf ts_bench_rocks ts_bench_kv
