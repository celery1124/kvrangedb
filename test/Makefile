
CC=g++
INCLUDES=-I../include
LIBS=-L../libs -pthread -lkvrangedb -std=c++11
CXXFLAG=-O2 -fPIC -w -march=native
CXXFLAG_DBG=-O0 -fPIC -w -march=native -g 

lsm:
	$(CC) db_test.cc -o db_test $(INCLUDES) $(LIBS) $(CXXFLAG_DBG) -DKVINDEX_LSM

btree:
	$(CC) db_test.cc -o db_test $(INCLUDES) $(LIBS) $(CXXFLAG_DBG) -DKVINDEX_BTREE

clean:
	rm -rf db_test
