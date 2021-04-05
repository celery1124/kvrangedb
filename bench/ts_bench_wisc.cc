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

#include "wisckey/slice.h"
#include "wisckey/db.h"

#define MAX_THREAD_CNT 64
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
  RandomGenerator(int seed) {
    Random rdn(seed);
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

// assume compression ratio = 0.5
void setValueBuffer(char* value_buf, int size,
		    std::mt19937_64 &e,
		    std::uniform_int_distribution<unsigned long long>& dist) {
    memset(value_buf, 0, size);
    int pos = size / 2;
    while (pos < size) {
	uint64_t num = dist(e);
	char* num_bytes = reinterpret_cast<char*>(&num);
	memcpy(value_buf + pos, num_bytes, 8);
	pos += 8;
    }
}


void buildVal (RandomGenerator& gen, int val_size, char *val) {

    char *val_buf = gen.Generate(val_size);
    memcpy(val, val_buf, val_size);
    
}

void Insert(wisckey::DB* db, std::vector<uint64_t>* keys,
		    uint64_t start, uint64_t query_count, int thread_cnt, uint64_t value_size) {
    
    RandomGenerator gen(start);
    char value_buf[1024];

    for (uint64_t i = start; i < query_count; i+=thread_cnt) {
	uint64_t key = (*keys)[i];
    key = htobe64(key);
    wisckey::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
    // setValueBuffer(value_buf, value_size, e, dist);
    buildVal(gen, value_size, value_buf);
    wisckey::Slice s_value(value_buf, value_size);

    wisckey::Status status = db->Put(wisckey::WriteOptions(), s_key, s_value);
    }

}


void load(const std::string& key_path, const std::string& db_path, wisckey::DB** db,
	  wisckey::Options* options, int use_direct_io, uint64_t key_count, uint64_t value_size,
	  int filter_type, int compression_type, int thread_cnt) {
    
    std::mt19937_64 e(2017);
    std::uniform_int_distribution<unsigned long long> dist(0, ULLONG_MAX);
    
    char value_buf[value_size];

	std::cout << "loading timestamp keys\n";
	std::ifstream keyFile(key_path);
	std::vector<uint64_t> keys;

	uint64_t key = 0;
	for (uint64_t i = 0; i < key_count; i++) {
	    keyFile >> key;
	    keys.push_back(key);
	}
    keyFile.close();

    wisckey::Status status;
	std::cout << "inserting keys\n";

    struct timespec ts_start;
    struct timespec ts_end;
    uint64_t elapsed;
    clock_gettime(CLOCK_MONOTONIC, &ts_start);

    // multi-thread load 
    std::thread *thrd[MAX_THREAD_CNT];
    for (int i = 0; i< thread_cnt; i++) {
        thrd[i] = new std::thread(Insert, *db, &keys, i, key_count, thread_cnt, value_size);
    }
    for (int i = 0; i< thread_cnt; i++) {
        thrd[i]->join();
    }

	// for (uint64_t i = 0; i < key_count; i++) {
	//     key = keys[i];
	//     key = htobe64(key);
	//     wisckey::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
	//     setValueBuffer(value_buf, value_size, e, dist);
	//     wisckey::Slice s_value(value_buf, value_size);

	//     status = (*db)->Put(wisckey::WriteOptions(), s_key, s_value);
	//     if (!status.ok()) {
	// 	assert(false);
	//     }

	//     if (i % (key_count / 100) == 0)
	// 	std::cout << i << "/" << key_count << " [" << ((i + 0.0)/(key_count + 0.0) * 100.) << "]\n";
	// }

    clock_gettime(CLOCK_MONOTONIC, &ts_end);
    elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_end.tv_nsec) -
	static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_start.tv_nsec);

    std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
    std::cout << "throughput: " << (static_cast<double>(key_count) / (static_cast<double>(elapsed) / 1000000000.)) << " ops/sec\n";

}

void close(wisckey::DB* db) {
    delete db;
}

void scanAll( wisckey::DB* db) {

    
    struct timespec ts_start;
    struct timespec ts_end;
    uint64_t elapsed;

    clock_gettime(CLOCK_MONOTONIC, &ts_start);

    
	wisckey::ReadOptions read_options = wisckey::ReadOptions();
	wisckey::Iterator* it = db->NewIterator(read_options);

    it->SeekToFirst();
    int totalKeys = 0;

    while (it->Valid()) {
        it->Next();
        totalKeys++;
    }
    
    clock_gettime(CLOCK_MONOTONIC, &ts_end);
    elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_end.tv_nsec) -
	static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_start.tv_nsec);

    delete it;

    std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
    std::cout << "total keys: " << totalKeys << "\n";
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

void warmup(std::vector<uint64_t>* keys, uint64_t warmup_query_count, uint64_t key_range, wisckey::DB* db) {
    std::mt19937_64 e(0);
    std::uniform_int_distribution<unsigned long long> dist(0, key_range);
    int empty_cnt = 0;
    
    struct timespec ts_start;
    struct timespec ts_end;
    //uint64_t elapsed;
    uint64_t key = 0;
    std::cout << "warming up\n";
    //clock_gettime(CLOCK_MONOTONIC, &ts_start);

    for (uint64_t i = 0; i < warmup_query_count; i++) {
	key = (*keys)[dist(e)%keys->size()];
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
    else empty_cnt++;
    }
    
    //clock_gettime(CLOCK_MONOTONIC, &ts_end);
    //elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
    //static_cast<uint64_t>(ts_end.tv_nsec) -
    //static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
    //static_cast<uint64_t>(ts_start.tv_nsec);

    //std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
    //std::cout << "throughput: " << (static_cast<double>(keys.size()) / (static_cast<double>(elapsed) / 1000000000.)) << "\n";
    // std::cout << "empty ratio: " << (double) empty_cnt / warmup_query_count << std::endl;
    std::cout << "warming up finished\n";
}

void benchPointQuery(wisckey::DB* db, wisckey::Options* options, int seed,
		     uint64_t key_range, uint64_t query_count, std::vector<uint64_t>* keys, int empty_ratio) {
    //std::random_device rd;
    //std::mt19937_64 e(rd());
    std::mt19937_64 e(seed);
    std::uniform_int_distribution<unsigned long long> dist(0, key_range);

    // struct timespec ts_start;
    // struct timespec ts_end;
    // uint64_t elapsed;

    // printf("point query\n");
    // clock_gettime(CLOCK_MONOTONIC, &ts_start);
    int empty_cnt = 0;
    for (uint64_t i = 0; i < query_count; i++) {
	uint64_t key = dist(e);
    if (key%100 > empty_ratio) key = (*keys)[i%keys->size()];
	key = htobe64(key);

	wisckey::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
	std::string s_value;
	uint64_t value;

    wisckey::ReadOptions rdopts;
	wisckey::Status status = db->Get(rdopts, s_key, &s_value);

	if (status.ok()) {
	    assert(s_value.size() >= sizeof(uint64_t));
	    value = *reinterpret_cast<const uint64_t*>(s_value.data());
	    (void)value;
	}
    else empty_cnt++;
    }
    
    // clock_gettime(CLOCK_MONOTONIC, &ts_end);
    // elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
	// static_cast<uint64_t>(ts_end.tv_nsec) -
	// static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
	// static_cast<uint64_t>(ts_start.tv_nsec);

    // std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
    // std::cout << "throughput: " << (static_cast<double>(query_count) / (static_cast<double>(elapsed) / 1000000000.)) << " ops/sec\n";
    std::cout << "empty ratio: " << (double) empty_cnt / query_count << std::endl;
}

void benchOpenRangeQuery(wisckey::DB* db, wisckey::Options* options, int seed, uint64_t key_range,
			 uint64_t query_count, uint64_t scan_length) {
    //std::random_device rd;
    //std::mt19937_64 e(rd());
    std::mt19937_64 e(seed);
    std::uniform_int_distribution<unsigned long long> dist(0, key_range);

    // struct timespec ts_start;
    // struct timespec ts_end;
    // uint64_t elapsed;

    // printf("open range query\n");
    
    // clock_gettime(CLOCK_MONOTONIC, &ts_start);

    for (uint64_t i = 0; i < query_count; i++) {
    wisckey::ReadOptions rdopt;
    rdopt.scan_length = scan_length;
    wisckey::Iterator* it = db->NewIterator(rdopt);

	uint64_t key = dist(e);
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
    delete it;
    }
    
    // clock_gettime(CLOCK_MONOTONIC, &ts_end);
    // elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
	// static_cast<uint64_t>(ts_end.tv_nsec) -
	// static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
	// static_cast<uint64_t>(ts_start.tv_nsec);

    // std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
    // std::cout << "throughput: " << (static_cast<double>(query_count) / (static_cast<double>(elapsed) / 1000000000.)) << " ops/sec\n";

    
}

void benchClosedRangeQuery(wisckey::DB* db, wisckey::Options* options,  int seed, uint64_t key_range,
			   uint64_t query_count, uint64_t range_size) {
    //std::random_device rd;
    //std::mt19937_64 e(rd());
    std::mt19937_64 e(seed);
    std::uniform_int_distribution<unsigned long long> dist(0, key_range);

    // struct timespec ts_start;
    // struct timespec ts_end;
    // uint64_t elapsed;

    // printf("closed range query\n");

    // clock_gettime(CLOCK_MONOTONIC, &ts_start);
    int closed_seek = 0;

    for (uint64_t i = 0; i < query_count; i++) {
	uint64_t key = dist(e);
	uint64_t upper_key = key + range_size;
	key = htobe64(key);
	wisckey::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
	upper_key = htobe64(upper_key);
	wisckey::Slice s_upper_key(reinterpret_cast<const char*>(&upper_key), sizeof(upper_key));
	
	std::string s_value;
	uint64_t value;

	wisckey::ReadOptions read_options = wisckey::ReadOptions();
	read_options.upper_key = &s_upper_key;
	wisckey::Iterator* it = db->NewIterator(read_options);

	uint64_t j = 0;
	for (it->Seek(s_key); it->Valid(); it->Next(), j++) {
	    uint64_t found_key = *reinterpret_cast<const uint64_t*>(it->key().data());
	    assert(it->value().size() >= sizeof(uint64_t));
	    value = *reinterpret_cast<const uint64_t*>(it->value().data());
	    (void)value;
	    // break;
	}
    if (j > 0) closed_seek++;
	
	delete it;
    }

    printf("closed seek %d/%d\n", closed_seek, query_count);
    
    // clock_gettime(CLOCK_MONOTONIC, &ts_end);
    // elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
	// static_cast<uint64_t>(ts_end.tv_nsec) -
	// static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
	// static_cast<uint64_t>(ts_start.tv_nsec);

    // std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
    // std::cout << "throughput: " << (static_cast<double>(query_count) / (static_cast<double>(elapsed) / 1000000000.)) << " ops/sec\n";

}

void printIO(char *prefix, char *dev) {
    fprintf(stderr, "%s: ", prefix);
    char dev_stat[256] = "/sys/block/nvme0n1/stat";
    memcpy(&(dev_stat[11]), dev, strlen(dev));
    // memcpy(&(dev_stat[19]), dev, strlen(dev));
    FILE* fp = fopen(dev_stat, "r");
    if (fp == NULL) {
    fprintf(stderr, "Error: empty fp\n");
    fprintf(stderr, "%s\n", strerror(errno));
    return;
    }
    char buf[4096];
    if (fgets(buf, sizeof(buf), fp) != NULL)
    fprintf(stderr, "%s", buf);
    fclose(fp);
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


int main(int argc, const char* argv[]) {
    if (argc < 12) {
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
	std::cout << "arg 4: use direct I/O?\n";
	std::cout << "\t0: no\n";
	std::cout << "\t1: yes\n";
	std::cout << "arg 5: query type\n";
	std::cout << "\t0: load\n";
	std::cout << "\t1: point query\n";
	std::cout << "\t2: open range query\n";
	std::cout << "\t3: closed range query\n";
	std::cout << "arg 6: range size\n";
	std::cout << "arg 7: warmup # of queries\n";
	std::cout << "arg 8: total # of keys\n";
	std::cout << "arg 9: total # of queries\n";
	std::cout << "arg 10: # of threads\n";
	std::cout << "arg 11: dev\n";
	std::cout << "arg 12: point query empty ratio 1/1000\n";

	return -1;
    }

    std::string db_path = std::string(argv[1]);
    int filter_type = atoi(argv[2]);
    int compression_type = atoi(argv[3]);
    int use_direct_io = atoi(argv[4]);
    int query_type = atoi(argv[5]);
    uint64_t range_size = (uint64_t)atoi(argv[6]);
    uint64_t warmup_query_count = (uint64_t)atoi(argv[7]);
    int thread_cnt = atoi(argv[10]);
    char *dev = (char *)argv[11];
    int pq_empty_ratio = atoi(argv[12]);
    int packSize = 4000;
    uint64_t scan_length = 10;
    scan_length = range_size;

    const std::string kKeyPath = "poisson_timestamps.csv";
    const uint64_t kValueSize = 1009;
    // const uint64_t kKeyRange = 10000000000000;
    const uint64_t kKeyRange = 50000000000000;
    uint64_t kQueryCount = 200000;
    // const uint64_t kQueryCount = 1;
    kQueryCount = (uint64_t)atoi(argv[9]);

    // 2GB config
    // const uint64_t kKeyCount = 2000000;
    // const uint64_t kWarmupSampleGap = 100;

    // 50GB config
    uint64_t kKeyCount = 500000000;
    kKeyCount = (uint64_t)atoi(argv[8]);
    const uint64_t kWarmupSampleGap = kKeyCount / kQueryCount;


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
    options.filterBitsPerKey = 16;
    options.dataCacheSize = 0;
    options.indexCacheSize = 512;
    
    wisckey::Status status = wisckey::DB::Open(options, db_path, &db);
    
    

    if (query_type == 0) {
        printIO("io_start", dev);
        load(kKeyPath, db_path, &db, &options, use_direct_io, kKeyCount, kValueSize, filter_type, compression_type, thread_cnt);
        // scanAll(db);
	    close(db);
        printIO("io_end", dev);
        return 0;
    }

    //=========================================================================

    //testScan(db, kKeyCount);

    uint64_t mem_free_before = getMemFree();
    uint64_t mem_available_before = getMemAvailable();
    //std::cout << options.statistics->ToString() << "\n";
    //printIO();

    // read keys for warmup and point query
    std::ifstream keyFile(kKeyPath);
    std::vector<uint64_t> hitKeys;
    uint64_t key = 0;
    for (uint64_t i = 0; i < kKeyCount; i++) {
	keyFile >> key;
	// if (i % kWarmupSampleGap == 0)
	    hitKeys.push_back(key);
    }
    keyFile.close();

    warmup(&hitKeys, warmup_query_count, kKeyRange, db);
    //warmup(kKeyPath, db, kKeyRange, kWarmupQueryCount);
    //std::cout << "read_count = " << (static_cast<double>(read_count - current_read_count) / kWarmupQueryCount) << " per op\n\n";

    //std::cout << options.statistics->ToString() << "\n";
    //printIO();

    //uint64_t mem_free_after = getMemFree();
    //uint64_t mem_available_after = getMemAvailable();
    //std::cout << "Mem Free diff: " << (mem_free_before - mem_free_after) << "\n";
    //std::cout << "Mem Aavilable diff: " << (mem_available_before - mem_available_after) << "\n";

    uint64_t io_before = getIOCount();
    //mem_free_before = getMemFree();
    //mem_available_before = getMemAvailable();

    struct timespec ts_start;
    struct timespec ts_end;
    uint64_t elapsed;
    printIO("io_start", dev);
    
    clock_gettime(CLOCK_MONOTONIC, &ts_start);

    std::thread *thrd[16];
    for (int i = 0; i< thread_cnt; i++) {
        if (query_type == 1)
        thrd[i] = new std::thread(benchPointQuery, db, &options, i, kKeyRange, kQueryCount, &hitKeys, pq_empty_ratio);
        else if (query_type == 2)
        thrd[i] = new std::thread(benchOpenRangeQuery, db, &options, i, kKeyRange, kQueryCount, scan_length);
        else if (query_type == 3)
        thrd[i] = new std::thread(benchClosedRangeQuery,db, &options, i, kKeyRange, kQueryCount, range_size);
        else 
        thrd[i] = new std::thread(benchPointQuery, db, &options, i, kKeyRange, kQueryCount, &hitKeys, pq_empty_ratio);
    }
    for (int i = 0; i< thread_cnt; i++) {
        thrd[i]->join();
    }

    clock_gettime(CLOCK_MONOTONIC, &ts_end);
    elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_end.tv_nsec) -
	static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_start.tv_nsec);

    std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
    std::cout << "throughput: " << (static_cast<double>(kQueryCount*thread_cnt) / (static_cast<double>(elapsed) / 1000000000.)) << " ops/sec\n";

    // if (query_type == 1)
	// benchPointQuery(db, &options, kKeyRange, kQueryCount);
    // else if (query_type == 2)
	// benchOpenRangeQuery(db, &options, kKeyRange, kQueryCount, scan_length);
    // else if (query_type == 3)
	// benchClosedRangeQuery(db, &options, kKeyRange, kQueryCount, range_size);

    uint64_t io_after = getIOCount();
    //mem_free_after = getMemFree();
    //mem_available_after = getMemAvailable();
    printIO("io_end", dev);

    
    // std::cout << "I/O count: " << (io_after - io_before) << "\n";
    //std::cout << "Mem Free diff: " << (mem_free_before - mem_free_after) << "\n";
    //std::cout << "Mem Aavilable diff: " << (mem_available_before - mem_available_after) << "\n";

    close(db);

    return 0;
}