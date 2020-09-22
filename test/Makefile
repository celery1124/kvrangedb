
CC=g++
INCLUDES=-I../include
LIBS=-L../libs -pthread -lkvrangedb -std=c++11
CXXFLAG=-O2 -fPIC -w -march=native
CXXFLAG_DBG=-O0 -fPIC -w -march=native -g 

all:
	$(CC) db_test.cc -o db_test $(INCLUDES) $(LIBS) $(CXXFLAG_DBG)
	$(CC) dev_getAllKeys.cc -o dev_getAllKeys $(INCLUDES) -L../libs -pthread -lkvssd -lkvapi -std=c++11 $(CXXFLAG_DBG)
clean:
	rm -rf db_test dev_getAllKeys
