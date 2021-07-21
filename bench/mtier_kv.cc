// date: 04/18/2021
// author: Mian Qin

#include <endian.h>
#include <errno.h>
#include <time.h>
#include <cinttypes>
#include <climits>
#include <cstdio>
#include <thread>
#include <atomic>
#include "unistd.h"

#include <iostream>
#include <fstream>
#include <random>
#include <string>
#include <vector>
#include <assert.h>
#include <iomanip>
#include <sstream>
#include <algorithm>

#include "kvrangedb/slice.h"
#include "kvrangedb/comparator.h"
#include "kvrangedb/db.h"


using namespace kvrangedb;

#define FNV_OFFSET_BASIS_64  0xCBF29CE484222325
#define FNV_OFFSET_BASIS_32  0x811c9dc5
#define FNV_PRIME_64  1099511628211L
#define FNV_PRIME_32 16777619
#define MAX_THREAD_CNT 64


int64_t fnvhash64(int64_t val) {
    //from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
    int64_t hashval = FNV_OFFSET_BASIS_64;

    for (int i = 0; i < 8; i++) {
      int64_t octet = val & 0x00ff;
      val = val >> 8;

      hashval = hashval ^ octet;
      hashval = hashval * FNV_PRIME_64;
      //hashval = hashval ^ octet;
    }
    return hashval >= 0 ? hashval : -hashval;
  }


class Random {
 private:
  uint32_t seed_;
 public:
  explicit Random(uint32_t s) : seed_(s & 0x7fffffffu) {
    // Avoid bad seeds.
    if (seed_ == 0 || seed_ == 2147483647L) {
      seed_ = 1;
    }
  }
  uint32_t Next() {
    static const uint32_t M = 2147483647L;   // 2^31-1
    static const uint64_t A = 16807;  // bits 14, 8, 7, 5, 2, 1, 0
    // We are computing
    //       seed_ = (seed_ * A) % M,    where M = 2^31-1
    //
    // seed_ must not be zero or M, or else all subsequent computed values
    // will be zero or M respectively.  For all other values, seed_ will end
    // up cycling through every number in [1,M-1]
    uint64_t product = seed_ * A;

    // Compute (product % M) using the fact that ((x << 31) % M) == x.
    seed_ = static_cast<uint32_t>((product >> 31) + (product & M));
    // The first reduction may overflow by 1 bit, so we may need to
    // repeat.  mod == M is not possible; using > allows the faster
    // sign-bit-based test.
    if (seed_ > M) {
      seed_ -= M;
    }
    return seed_;
  }
  // Returns a uniformly distributed value in the range [0..n-1]
  // REQUIRES: n > 0
  uint32_t Uniform(int n) { return Next() % n; }

  // Randomly returns true ~"1/n" of the time, and false otherwise.
  // REQUIRES: n > 0
  bool OneIn(int n) { return (Next() % n) == 0; }

  // Skewed: pick "base" uniformly from range [0,max_log] and then
  // return "base" random bits.  The effect is to pick a number in the
  // range [0,2^max_log-1] with exponential bias towards smaller numbers.
  uint32_t Skewed(int max_log) {
    return Uniform(1 << Uniform(max_log + 1));
  }
};

class RandomGenerator {
 private:
  std::string data_;
  int pos_;

 public:
  RandomGenerator() {
    Random rdn(0);
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    while (data_.size() < 1048576) {
      // Add a short fragment that is as compressible as specified
      // by FLAGS_compression_ratio.
      data_.append(1, (char)(' '+rdn.Uniform(95)));
    }
    pos_ = 0;
  }

  char* Generate(size_t len) {
    if (pos_ + len > data_.size()) {
      pos_ = 0;
      assert(len < data_.size());
    }
    pos_ += len;
    return (char *)(data_.data() + pos_ - len);
  }
};

std::string buildKey (int key_size, uint64_t num) {

    std::stringstream stream;
    stream << std::hex << num;
    std::string key = stream.str();
    int size = key.size();

    // for (int i = 0; i < key_size-size; i++) {
    //     key = "0" + key;
    // }
    return key;

}

void buildVal (RandomGenerator& gen, int val_size, std::string& val) {
    //field
    int field_len = 4;
    val.resize(14 + val_size);
    char *p = (char *)&val[0];
    // memcpy(p, &field_len, 4);
    // p += 4;
    // memcpy(p, "field0", 6);
    // p += 6;
    // memcpy(p, &val_size, 4);
    // p += 4;

    char *val_buf = gen.Generate(val_size);
    memcpy(p, val_buf, val_size);
    
}

void close(kvrangedb::DB* db) {
    delete db;
}


void benchInsert(kvrangedb::DB* db, uint64_t start, uint64_t query_count, int val_size) {

    RandomGenerator gen;
    Random rand(start);

    for (uint64_t i = start; i < start+query_count; i++) {
	uint64_t r = fnvhash64(i);
	std::string str_key = buildKey(16, r);
	std::string str_value;
    buildVal(gen, val_size, str_value);
    kvrangedb::Slice s_key(str_key);
    kvrangedb::Slice s_value(str_value);

    kvrangedb::Status status = db->Put(kvrangedb::WriteOptions(), s_key, s_value);
    }

}


void load(const std::string& db_path, kvrangedb::DB** db, uint64_t start,
	  int thread_cnt, uint64_t kInsertCount, int val_size) {
    
    std::thread *thrd[MAX_THREAD_CNT];
    for (int i = 0; i< thread_cnt; i++) {
        thrd[i] = new std::thread(benchInsert, *db, start+kInsertCount*i, kInsertCount, val_size);
    }
    for (int i = 0; i< thread_cnt; i++) {
        thrd[i]->join();
    }

}

void warmup(const std::string key_path, uint64_t key_count, uint64_t sample_gap, kvrangedb::DB* db) {
    std::ifstream keyFile(key_path);
    std::vector<uint64_t> keys;
    uint64_t key = 0;
    for (uint64_t i = 0; i < key_count; i++) {
	keyFile >> key;
	if (i % sample_gap == 0)
	    keys.push_back(key);
    }
    
    struct timespec ts_start;
    struct timespec ts_end;
    //uint64_t elapsed;

    //std::cout << "warming up\n";
    //clock_gettime(CLOCK_MONOTONIC, &ts_start);

    for (uint64_t i = 0; i < keys.size(); i++) {
	key = keys[i];
	key = htobe64(key);

	kvrangedb::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
	std::string s_value;
	uint64_t value;

	kvrangedb::Status status = db->Get(kvrangedb::ReadOptions(), s_key, &s_value);

	if (status.ok()) {
	    assert(s_value.size() >= sizeof(uint64_t));
	    value = *reinterpret_cast<const uint64_t*>(s_value.data());
	    (void)value;
	}
    }
    
    //clock_gettime(CLOCK_MONOTONIC, &ts_end);
    //elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
    //static_cast<uint64_t>(ts_end.tv_nsec) -
    //static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
    //static_cast<uint64_t>(ts_start.tv_nsec);

    //std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
    //std::cout << "throughput: " << (static_cast<double>(keys.size()) / (static_cast<double>(elapsed) / 1000000000.)) << "\n";
}

void benchPointQuery(kvrangedb::DB* db, kvrangedb::Options* options,
		     uint64_t start, uint64_t query_count, int hint_pack) {


    // printf("point query\n");

    for (uint64_t i = start; i < query_count+start; i++) {
	uint64_t key = fnvhash64(i);
	std::string str_key = buildKey(16, key);

	kvrangedb::Slice s_key(str_key);
	std::string s_value;

    kvrangedb::ReadOptions rdopts;
    rdopts.hint_packed = hint_pack;
	kvrangedb::Status status = db->Get(rdopts, s_key, &s_value);

	if (status.ok()) {
	    //printf("value size: %d\n", s_value.size());
	}
    else {
        //std::cout << "key: " << str_key << " not found\n";
    }
    }

}

void benchOpenRangeQuery(kvrangedb::DB* db, kvrangedb::Options* options, uint64_t key_range,
			 uint64_t query_count, uint64_t scan_length) {
    //std::random_device rd;
    //std::mt19937_64 e(rd());
    std::mt19937_64 e(2017);
    std::uniform_int_distribution<unsigned long long> dist(0, key_range);

    std::vector<uint64_t> query_keys;

    for (uint64_t i = 0; i < query_count; i++) {
	uint64_t r = dist(e);
	query_keys.push_back(r);
    }

    struct timespec ts_start;
    struct timespec ts_end;
    uint64_t elapsed;

    printf("open range query\n");
    kvrangedb::Iterator* it = db->NewIterator(kvrangedb::ReadOptions());

    clock_gettime(CLOCK_MONOTONIC, &ts_start);

    for (uint64_t i = 0; i < query_count; i++) {
	uint64_t key = query_keys[i];
	key = htobe64(key);
	kvrangedb::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
	
	std::string s_value;
	uint64_t value;

	uint64_t j = 0;
	for (it->Seek(s_key); it->Valid() && j < scan_length; it->Next(), j++) {
	    uint64_t found_key = *reinterpret_cast<const uint64_t*>(it->key().data());
	    assert(it->value().size() >= sizeof(uint64_t));
	    value = *reinterpret_cast<const uint64_t*>(it->value().data());
	    (void)value;
	    // break;
	}
    }
    
    clock_gettime(CLOCK_MONOTONIC, &ts_end);
    elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_end.tv_nsec) -
	static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_start.tv_nsec);

    std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
    std::cout << "throughput: " << (static_cast<double>(query_count) / (static_cast<double>(elapsed) / 1000000000.)) << " ops/sec\n";

    delete it;
}

void benchClosedRangeQuery(kvrangedb::DB* db, kvrangedb::Options* options, uint64_t start,
			   uint64_t query_count, uint64_t range_size) {

    // printf("closed range query\n");
    int closed_seek = 0;

    for (uint64_t i = start; i < query_count+start; i++) {
	uint64_t key = fnvhash64(i+1000000000);
    std::string str_key = buildKey(16, key);
    std::string str_upper_key (str_key);
    std::reverse(str_upper_key.begin(), str_upper_key.end());

	kvrangedb::Slice s_key(str_key);
    uint32_t *upper_key = (uint32_t *)str_upper_key.data() ;
    *upper_key = *upper_key + range_size;
    std::reverse(str_upper_key.begin(), str_upper_key.end());

	kvrangedb::Slice s_upper_key(str_upper_key);

	kvrangedb::ReadOptions read_options = kvrangedb::ReadOptions();
	read_options.upper_key = &s_upper_key;
	kvrangedb::Iterator* it = db->NewIterator(read_options);

	uint64_t j = 0;
	for (it->Seek(s_key); it->Valid(); it->Next(), j++) {
	    kvrangedb::Slice found_key = it->key();
        closed_seek++;
        // break;
	}
	
	delete it;
    }
    printf("closed seek %d/%d\n", closed_seek, query_count);
    
}

void printIO() {
    return;
}

uint64_t getIOCount() {
    return 0;
}

uint64_t getMemFree() {
    std::ifstream mem_file(std::string("/proc/meminfo"));
    std::string str;
    uint64_t free_mem = 0;
    for (int i = 0; i < 4; i++)
	mem_file >> str;
    mem_file >> free_mem;
    return free_mem;
}

uint64_t getMemAvailable() {
    std::ifstream mem_file(std::string("/proc/meminfo"));
    std::string str;
    uint64_t mem_available = 0;
    for (int i = 0; i < 7; i++)
	mem_file >> str;
    mem_file >> mem_available;
    return mem_available;
}


class CustomComparator : public kvrangedb::Comparator {
public:
  CustomComparator() {}
  ~CustomComparator() {}
  int Compare(const kvrangedb::Slice& a, const kvrangedb::Slice& b) const {
    return a.compare(b);
  }
};

int main(int argc, const char* argv[]) {
    if (argc <= 7) {
	std::cout << "Usage:\n";
	std::cout << "arg 1: path to datafiles\n";
	std::cout << "arg 2: value size\n";
	std::cout << "arg 3: pack number\n";
    std::cout << "arg 4: total keys\n";
    std::cout << "arg 5: query keys\n";
    std::cout << "arg 6: # of write threads\n";
    std::cout << "arg 7: # of read threads\n";
	return -1;
    }

    std::string db_path = std::string(argv[1]);
    int val_size = (int)atoi(argv[2]);
    int pack_num = (int)atoi(argv[3]);
    uint64_t total_counts = (uint64_t)atoi(argv[4]);
    uint64_t query_counts = (uint64_t)atoi(argv[5]);
    int thread_cnt = (int)atoi(argv[6]);
    int read_thread_cnt = (int)atoi(argv[7]);


    //=========================================================================
    
    kvrangedb::DB* db;
    kvrangedb::Options options;

    CustomComparator cmp;
    options.comparator = &cmp;
    options.indexNum = 1;
    options.indexType = kvrangedb::ROCKS;
    options.packThreadsNum = 12;
    options.packThres = 10;
    options.indexCacheSize = 1;
    options.statistics = kvrangedb::Options::CreateDBStatistics();
    
    kvrangedb::Status status = kvrangedb::DB::Open(options, db_path, &db);

    
    struct timespec ts_start;
    struct timespec ts_end;
    uint64_t elapsed;

    //========================== load phase =================================//
    // target total keys 1 Billion
    uint64_t perThreadInsertCount;
    clock_gettime(CLOCK_MONOTONIC, &ts_start);
    perThreadInsertCount = uint64_t(total_counts * 0.1) / thread_cnt; // target on 100M

    // load upacked
    load(db_path, &db, 0, thread_cnt, perThreadInsertCount, val_size);
    printf("Finshed loading unpacked objects (%d) \n", perThreadInsertCount*thread_cnt);

    clock_gettime(CLOCK_MONOTONIC, &ts_end);
    elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_end.tv_nsec) -
	static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_start.tv_nsec);

    std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
    std::cout << "throughput: " << (static_cast<double>(perThreadInsertCount*thread_cnt) / (static_cast<double>(elapsed) / 1000000000.)) << " ops/sec\n";
    close(db);


    // load packed
    perThreadInsertCount = uint64_t(total_counts * 0.9) / thread_cnt; // target on 900M
    if (pack_num > 1) {
        options.packThres = 2000;
        options.packSize = 65536;
        options.maxPackNum = pack_num;
    }
    
    status = kvrangedb::DB::Open(options, db_path, &db);

    clock_gettime(CLOCK_MONOTONIC, &ts_start);
    load(db_path, &db, uint64_t(total_counts * 0.1), thread_cnt, perThreadInsertCount, val_size);
    printf("Finshed loading packed [%d] objects (%d) \n", pack_num, perThreadInsertCount*thread_cnt);

    clock_gettime(CLOCK_MONOTONIC, &ts_end);
    elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_end.tv_nsec) -
	static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_start.tv_nsec);

    std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
    std::cout << "throughput: " << (static_cast<double>(perThreadInsertCount*thread_cnt) / (static_cast<double>(elapsed) / 1000000000.)) << " ops/sec\n";

    uint64_t mem_free_before = getMemFree();
    uint64_t mem_available_before = getMemAvailable();
    
    sleep(10);
    
    clock_gettime(CLOCK_MONOTONIC, &ts_start);
    uint64_t perThreadQueryCount  = query_counts/read_thread_cnt;

    std::thread *thrd[MAX_THREAD_CNT];
    for (int i = 0; i< read_thread_cnt; i++) {  
        thrd[i] = new std::thread(benchPointQuery, db, &options, i*perThreadQueryCount, perThreadQueryCount, 1);
    }
    for (int i = 0; i< read_thread_cnt; i++) {
        thrd[i]->join();
    }

    clock_gettime(CLOCK_MONOTONIC, &ts_end);
    elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_end.tv_nsec) -
	static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_start.tv_nsec);

    std::cout << "pointQuery bench\n";
    std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
    std::cout << "throughput: " << (static_cast<double>(query_counts) / (static_cast<double>(elapsed) / 1000000000.)) << " ops/sec\n";


    clock_gettime(CLOCK_MONOTONIC, &ts_start);

    std::thread *thrd2[MAX_THREAD_CNT];
    for (int i = 0; i< read_thread_cnt; i++) {  
        thrd2[i] = new std::thread(benchPointQuery, db, &options, uint64_t(total_counts * 0.1)+i*perThreadQueryCount, perThreadQueryCount, 2);
    }
    for (int i = 0; i< read_thread_cnt; i++) {
        thrd2[i]->join();
    }

    clock_gettime(CLOCK_MONOTONIC, &ts_end);
    elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_end.tv_nsec) -
	static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_start.tv_nsec);

    std::cout << "pointQuery bench packed\n";
    std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
    std::cout << "throughput: " << (static_cast<double>(query_counts) / (static_cast<double>(elapsed) / 1000000000.)) << " ops/sec\n";

    //mem_free_after = getMemFree();
    //mem_available_after = getMemAvailable();
    //std::cout << options.statistics->ToString() << "\n";
    //std::string stats;
    //db->GetProperty(rocksdb::Slice("rocksdb.stats"), &stats);
    //std::cout << stats << "\n";
    //printIO();

    //std::cout << "Mem Free diff: " << (mem_free_before - mem_free_after) << "\n";
    //std::cout << "Mem Aavilable diff: " << (mem_available_before - mem_available_after) << "\n";

    close(db);

    return 0;
}