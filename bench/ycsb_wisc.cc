// data: 11/18/2020
// author: Mian Qin

#include <endian.h>
#include <errno.h>
#include <time.h>
#include <cinttypes>
#include <climits>
#include <cstdio>
#include <thread>
#include <atomic>

#include <iostream>
#include <fstream>
#include <random>
#include <string>
#include <vector>
#include <assert.h>
#include <iomanip>
#include <sstream>
#include <algorithm>

#include "wisckey/slice.h"
#include "wisckey/db.h"

using namespace wisckey;

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

    for (int i = 0; i < key_size-size; i++) {
        key = "0" + key;
    }
    return key;

}

void buildVal (RandomGenerator& gen, int val_size, std::string& val) {
    //field
    int field_len = 4;
    val.resize(14 + val_size);
    char *p = (char *)&val[0];
    memcpy(p, &field_len, 4);
    p += 4;
    memcpy(p, "field0", 6);
    p += 6;
    memcpy(p, &val_size, 4);
    p += 4;

    char *val_buf = gen.Generate(val_size);
    memcpy(p, val_buf, val_size);
    
}


void close(wisckey::DB* db) {
    delete db;
}

void testScan(const std::string& key_path, wisckey::DB* db, uint64_t key_count) {
    std::cout << "testScan: loading timestamp keys\n";
    std::ifstream keyFile(key_path);
    std::vector<uint64_t> keys;

    uint64_t key = 0;
    for (uint64_t i = 0; i < key_count; i++) {
	keyFile >> key;
	keys.push_back(key);
    }
    
    struct timespec ts_start;
    struct timespec ts_end;
    uint64_t elapsed;

    clock_gettime(CLOCK_MONOTONIC, &ts_start);

    for (uint64_t i = 0; i < key_count; i++) {
	key = htobe64(keys[i]);

	wisckey::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
	std::string s_value;
	uint64_t value;

	wisckey::Status status = db->Get(wisckey::ReadOptions(), s_key, &s_value);

	if (status.ok()) {
	    assert(s_value.size() >= sizeof(uint64_t));
	    value = *reinterpret_cast<const uint64_t*>(s_value.data());
	    (void)value;
	}
    }
    
    clock_gettime(CLOCK_MONOTONIC, &ts_end);
    elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_end.tv_nsec) -
	static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_start.tv_nsec);

    std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
    std::cout << "throughput: " << (static_cast<double>(key_count) / (static_cast<double>(elapsed) / 1000000000.)) << "\n";
}

void warmup(const std::string key_path, uint64_t key_count, uint64_t sample_gap, wisckey::DB* db) {
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

	wisckey::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
	std::string s_value;
	uint64_t value;

	wisckey::Status status = db->Get(wisckey::ReadOptions(), s_key, &s_value);

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

void benchPointQuery(wisckey::DB* db, wisckey::Options* options,
		     uint64_t start, uint64_t query_count) {


    // printf("point query\n");

    for (uint64_t i = start; i < query_count+start; i++) {
	uint64_t key = fnvhash64(i+1000000000);
	std::string str_key = buildKey(16, key);

	wisckey::Slice s_key(str_key);
	std::string s_value;

    wisckey::ReadOptions rdopts;
	wisckey::Status status = db->Get(rdopts, s_key, &s_value);

	if (status.ok()) {
	    //printf("value size: %d\n", s_value.size());
	}
    else {
        //std::cout << "key: " << str_key << " not found\n";
    }
    }

}

void benchOpenRangeQuery(wisckey::DB* db, wisckey::Options* options, uint64_t key_range,
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
    wisckey::Iterator* it = db->NewIterator(wisckey::ReadOptions());

    clock_gettime(CLOCK_MONOTONIC, &ts_start);

    for (uint64_t i = 0; i < query_count; i++) {
	uint64_t key = query_keys[i];
	key = htobe64(key);
	wisckey::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
	
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

void benchClosedRangeQuery(wisckey::DB* db, wisckey::Options* options, uint64_t start,
			   uint64_t query_count, uint64_t range_size) {

    // printf("closed range query\n");
    int closed_seek = 0;

    for (uint64_t i = start; i < query_count+start; i++) {
	uint64_t key = fnvhash64(i+1000000000);
    std::string str_key = buildKey(16, key);
    std::string str_upper_key (str_key);
    std::reverse(str_upper_key.begin(), str_upper_key.end());

	wisckey::Slice s_key(str_key);
    uint32_t *upper_key = (uint32_t *)str_upper_key.data() ;
    *upper_key = *upper_key + range_size;
    std::reverse(str_upper_key.begin(), str_upper_key.end());

	wisckey::Slice s_upper_key(str_upper_key);

	wisckey::ReadOptions read_options = wisckey::ReadOptions();
	read_options.upper_key = &s_upper_key;
	wisckey::Iterator* it = db->NewIterator(read_options);

	uint64_t j = 0;
	for (it->Seek(s_key); it->Valid(); it->Next(), j++) {
	    wisckey::Slice found_key = it->key();
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


void benchInsert(wisckey::DB* db, wisckey::Options* options,
		    uint64_t start, uint64_t query_count, int val_size) {

    RandomGenerator gen;
    Random rand(start);

    for (uint64_t i = start; i < start+query_count; i++) {
	uint64_t r = fnvhash64(i);
	std::string str_key = buildKey(16, r);
	std::string str_value;
    buildVal(gen, val_size, str_value);
    wisckey::Slice s_key(str_key);
    wisckey::Slice s_value(str_value);

    wisckey::Status status = db->Put(wisckey::WriteOptions(), s_key, s_value);
    }

}

void load(wisckey::DB* db,
	  wisckey::Options* options, int thread_cnt, int kInsertCount, int val_size) {

    std::thread *thrd[MAX_THREAD_CNT];
    for (int i = 0; i< thread_cnt; i++) {
        thrd[i] = new std::thread(benchInsert, db, options, kInsertCount*i, kInsertCount, val_size);
    }
    for (int i = 0; i< thread_cnt; i++) {
        thrd[i]->join();
    }

}


int main(int argc, const char* argv[]) {
    if (argc < 9) {
	std::cout << "Usage:\n";
	std::cout << "arg 1: path to datafiles\n";
	std::cout << "arg 2: filter type\n";
	std::cout << "\t0: no filter\n";
	std::cout << "\t1: Bloom filter\n";
	std::cout << "\t2: SuRF\n";
	std::cout << "\t3: SuRF Hash\n";
	std::cout << "\t4: SuRF Real\n";
	std::cout << "arg 3: compression?\n";
	std::cout << "\t0: no compression\n";
	std::cout << "\t1: Snappy\n";
	std::cout << "arg 4: index cache size (MB)\n";
	std::cout << "arg 5: query type\n";
	std::cout << "\t0: load\n";
	std::cout << "\t1: point query\n";
	std::cout << "\t2: open range query\n";
	std::cout << "\t3: closed range query\n";
	std::cout << "arg 6: range size\n";
	std::cout << "arg 7: # of queries\n";
    std::cout << "arg 8: kv pack hint\n";
    std::cout << "arg 9: # of threads\n";
    std::cout << "arg 10: total keys\n";
    std::cout << "arg 11: bits per key\n";
	return -1;
    }


    const uint64_t kKeyRange = 10000000000000;
    uint64_t kQueryCount = 200000;

    std::string db_path = std::string(argv[1]);
    int filter_type = atoi(argv[2]);
    int compression_type = atoi(argv[3]);
    int index_cache_size = atoi(argv[4]);
    int query_type = atoi(argv[5]);
    uint64_t range_size = (uint64_t)atoi(argv[6]);
    kQueryCount = (uint64_t)atoi(argv[7]);
    uint64_t warmup_query_count = 100000;
    int hint_packed = (int)atoi(argv[8]);
    int thread_cnt = (int)atoi(argv[9]);
    int val_size = (int)atoi(argv[10]);
    int bits_per_key = (int)atoi(argv[11]);
    uint64_t scan_length = 10;

    // 2GB config
    // const uint64_t kKeyCount = 2000000;
    // const uint64_t kWarmupSampleGap = 100;

    // 50GB config
    const uint64_t kKeyCount = 50000000;
    const uint64_t kWarmupSampleGap = kKeyCount / warmup_query_count;


    // 100GB config
    //const uint64_t kKeyCount = 100000000;
    //const uint64_t kWarmupSampleGap = kKeyCount / warmup_query_count;

    //=========================================================================
    
    wisckey::DB* db;
    wisckey::Options options;
    
    options.stats_dump_interval = 600;
    options.statistics = wisckey::Options::CreateDBStatistics();
    if (filter_type == 1) 
    options.filterType = wisckey::Bloom;
    else if (filter_type == 2)
    options.filterType = wisckey::Surf;
    options.filterBitsPerKey = bits_per_key;

    options.indexCacheSize = index_cache_size;
    options.logBufSize = 1<<20;
    options.walBufSize = 1<<20;
    
    wisckey::Status status = wisckey::DB::Open(options, db_path, &db);
    
    

    if (query_type == 0) {
        struct timespec ts_start;
        struct timespec ts_end;
        uint64_t elapsed;
        printf("Load database, #keys %d\n", thread_cnt*kQueryCount);
        clock_gettime(CLOCK_MONOTONIC, &ts_start);

        load(db, &options, thread_cnt, kQueryCount, val_size);

        clock_gettime(CLOCK_MONOTONIC, &ts_end);
        elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
        static_cast<uint64_t>(ts_end.tv_nsec) -
        static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
        static_cast<uint64_t>(ts_start.tv_nsec);

        std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
        std::cout << "throughput: " << (static_cast<double>(kQueryCount*thread_cnt) / (static_cast<double>(elapsed) / 1000000000.)) << " ops/sec\n";

        close(db);
        return 0;
    }

    //=========================================================================

    //testScan(db, kKeyCount);

    uint64_t mem_free_before = getMemFree();
    uint64_t mem_available_before = getMemAvailable();
    //std::cout << options.statistics->ToString() << "\n";
    //printIO();
    //warmup(kKeyPath, kKeyCount, kWarmupSampleGap, db);
    //warmup(kKeyPath, db, kKeyRange, kWarmupQueryCount);
    //std::cout << "read_count = " << (static_cast<double>(read_count - current_read_count) / kWarmupQueryCount) << " per op\n\n";

    //std::cout << options.statistics->ToString() << "\n";
    //printIO();

    //uint64_t mem_free_after = getMemFree();
    //uint64_t mem_available_after = getMemAvailable();
    //std::cout << "Mem Free diff: " << (mem_free_before - mem_free_after) << "\n";
    //std::cout << "Mem Aavilable diff: " << (mem_available_before - mem_available_after) << "\n";

    //mem_free_before = getMemFree();
    //mem_available_before = getMemAvailable();
    
    struct timespec ts_start;
    struct timespec ts_end;
    uint64_t elapsed;
    
    clock_gettime(CLOCK_MONOTONIC, &ts_start);

    std::thread *thrd[MAX_THREAD_CNT];
    for (int i = 0; i< thread_cnt; i++) {  
        // if (query_type == 1)
        thrd[i] = new std::thread(benchPointQuery, db, &options, i*kQueryCount, kQueryCount);
        // else if (query_type == 2)
        // thrd[i] = new std::thread(benchOpenRangeQuery, db, &options, kKeyRange, kQueryCount, scan_length);
        // else if (query_type == 3)
        // thrd[i] = new std::thread(benchClosedRangeQuery, db, &options, kKeyRange, kQueryCount, range_size);
        // else 
        // thrd[i] = new std::thread(benchPointQuery, db, &options, kKeyRange, kQueryCount);
    }
    for (int i = 0; i< thread_cnt; i++) {
        thrd[i]->join();
    }

    clock_gettime(CLOCK_MONOTONIC, &ts_end);
    elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_end.tv_nsec) -
	static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_start.tv_nsec);

    std::cout << "pointQuery bench\n";
    std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
    std::cout << "throughput: " << (static_cast<double>(kQueryCount*thread_cnt) / (static_cast<double>(elapsed) / 1000000000.)) << " ops/sec\n";

    options.statistics.get()->reportStats();

    std::thread *thrd2[MAX_THREAD_CNT];

    std::vector<int> range_size_list {10, 100, 500};
    std::vector<int> queryRaito {2, 10, 40};
    for (int r = 0 ; r < range_size_list.size(); r++) {
        clock_gettime(CLOCK_MONOTONIC, &ts_start);
        std::cout <<"Range distance: " << range_size_list[r] << "\n";
        for (int i = 0; i< thread_cnt; i++) {  
            // if (query_type == 1)
            // thrd2[i] = new std::thread(benchPointQuery, db, &options, kKeyRange, kQueryCount);
            // else if (query_type == 2)
            // thrd2[i] = new std::thread(benchOpenRangeQuery, db, &options, kKeyRange, kQueryCount, scan_length);
            // else if (query_type == 3)
            thrd2[i] = new std::thread(benchClosedRangeQuery, db, &options, i*kQueryCount, kQueryCount/queryRaito[r], range_size_list[r]);
            // else 
            // thrd2[i] = new std::thread(benchPointQuery, db, &options, kKeyRange, kQueryCount);
        }
        for (int i = 0; i< thread_cnt; i++) {
            thrd2[i]->join();
        }
        for (int i = 0; i < thread_cnt; i++) {
            delete thrd2[i];
        }
   

        clock_gettime(CLOCK_MONOTONIC, &ts_end);
        elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
        static_cast<uint64_t>(ts_end.tv_nsec) -
        static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
        static_cast<uint64_t>(ts_start.tv_nsec);

        std::cout << "closeQuery bench\n";
        std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
        std::cout << "throughput: " << (static_cast<double>(kQueryCount/queryRaito[r]*thread_cnt) / (static_cast<double>(elapsed) / 1000000000.)) << " ops/sec\n";

        options.statistics.get()->reportStats();
        options.statistics.get()->Reset();
    }
    //mem_free_after = getMemFree();
    //mem_available_after = getMemAvailable();
    //std::cout << options.statistics->ToString() << "\n";
    //std::string stats;
    //db->GetProperty(wisckey::Slice("wisckey.stats"), &stats);
    //std::cout << stats << "\n";
    //printIO();

    //std::cout << "Mem Free diff: " << (mem_free_before - mem_free_after) << "\n";
    //std::cout << "Mem Aavilable diff: " << (mem_available_before - mem_available_after) << "\n";

    close(db);

    return 0;
}