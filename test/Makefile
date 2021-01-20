
CC=g++
INCLUDES=-I../include
LIBS=-L../libs -pthread -lkvrangedb -lkvapi -std=c++11
CXXFLAG=-O2 -fPIC -w -march=native
CXXFLAG_DBG=-O0 -fPIC -w -march=native -g 

db_perf:
	$(CC) db_perf.cc -o db_perf $(INCLUDES) $(LIBS) $(CXXFLAG)

db_test:
	$(CC) db_test.cc -o db_test $(INCLUDES) $(LIBS) $(CXXFLAG)

bloom_test:
	$(CC) bloom_test.cc ../src/hash.cc -o bloom_test -I../src/ -I../include/ -std=c++11  -pthread -g -O3

all:
	$(CC) db_test.cc -o db_test $(INCLUDES) $(LIBS) $(CXXFLAG_DBG)
	$(CC) dev_getAllKeys.cc -o dev_getAllKeys $(INCLUDES) -L../libs -pthread -lkvssd -lkvapi -std=c++11 $(CXXFLAG_DBG)
clean:
	rm -rf db_test dev_getAllKeys bloom_test
