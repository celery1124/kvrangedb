// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

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

#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"

#define MAX_THREAD_CNT 64
//#include "rocksdb/utilities/leveldb_options.h"

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
    // int pos = size / 2;
    int pos = 0;
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

void Insert(rocksdb::DB* db, std::vector<uint64_t>* keys,
		    uint64_t start, uint64_t query_count, int thread_cnt, uint64_t value_size) {
    
    RandomGenerator gen(start);
    char value_buf[1024];

    printf("start from %lu, end %lu\n", start, start+query_count);
    for (uint64_t i = start; i < query_count; i+=thread_cnt) {
	uint64_t key = (*keys)[i];
    // printf("[%d] key : %lu, val size: %d\n", start, key, value_size);
    key = htobe64(key);
    rocksdb::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
    // setValueBuffer(value_buf, value_size, e, dist);
    buildVal(gen, value_size, value_buf);
    rocksdb::Slice s_value(value_buf, value_size);

    rocksdb::Status status = db->Put(rocksdb::WriteOptions(), s_key, s_value);
    }

}

void init(const std::string& key_path, const std::string& db_path, rocksdb::DB** db,
	  rocksdb::Options* options, rocksdb::BlockBasedTableOptions* table_options,
	  int block_cache_size, uint64_t key_count, uint64_t value_size,
	  int filter_type, int compression_type, int thread_cnt, int index_type) {
    
    std::mt19937_64 e(2017);
    std::uniform_int_distribution<unsigned long long> dist(0, ULLONG_MAX);

    if (filter_type == 1)
	table_options->filter_policy.reset(rocksdb::NewBloomFilterPolicy(16, false));
    else if (filter_type == 2)
	table_options->filter_policy.reset(rocksdb::NewSuRFPolicy(0, 0, true, 16, false));
    else if (filter_type == 3)
	table_options->filter_policy.reset(rocksdb::NewSuRFPolicy(1, 4, true, 16, false));
    else if (filter_type == 4)
	table_options->filter_policy.reset(rocksdb::NewSuRFPolicy(2, 4, true, 16, false));

    if (table_options->filter_policy == nullptr)
	std::cout << "Filter DISABLED\n";
    else
	std::cout << "Using " << table_options->filter_policy->Name() << "\n";

    if (compression_type == 0) {
	options->compression = rocksdb::CompressionType::kNoCompression;
	std::cout << "No Compression\n";
    } else if (compression_type == 1) {
	options->compression = rocksdb::CompressionType::kSnappyCompression;
	std::cout << "Snappy Compression\n";
    }

    //table_options->block_cache = rocksdb::NewLRUCache(10 * 1048576);
    table_options->block_cache = rocksdb::NewLRUCache((size_t)block_cache_size * 1048576);

    table_options->pin_l0_filter_and_index_blocks_in_cache = true;
    if (index_type == 0) {
        printf("fixed block cache size %d MB\n", block_cache_size);
        table_options->cache_index_and_filter_blocks = true;
    }
    else if (index_type == 1) {
        printf("cache all index/filter blocks\n");
        table_options->cache_index_and_filter_blocks = false;
    }
    else {
        table_options->cache_index_and_filter_blocks = true;
    }

    options->table_factory.reset(rocksdb::NewBlockBasedTableFactory(*table_options));

    options->max_open_files = -1; // pre-load indexes and filters

    // 2GB config
    // options->write_buffer_size = 2 * 1048576;
    // options->max_bytes_for_level_base = 10 * 1048576;
    // options->target_file_size_base = 2 * 1048576;

    // 100GB config
    options->write_buffer_size = 64 * 1048576;
    options->max_bytes_for_level_base = 256 * 1048576;
    options->target_file_size_base = 64 * 1048576;

    options->allow_mmap_reads = false;
	options->allow_mmap_writes = false;
	options->use_direct_reads = true;
    options->use_direct_io_for_flush_and_compaction=true;

    options->statistics = rocksdb::CreateDBStatistics();
    options->IncreaseParallelism();
    options->OptimizeLevelStyleCompaction();

    //options->create_if_missing = false;
    //options->error_if_exists = false;
    
    //options->prefix_extractor = nullptr;
    //options->disable_auto_compactions = false;
    rocksdb::Status status = rocksdb::DB::Open(*options, db_path, db);
    if (!status.ok()) {
	std::cout << "creating new DB\n";
	options->create_if_missing = true;
	status = rocksdb::DB::Open(*options, db_path, db);

	if (!status.ok()) {
	    std::cout << status.ToString().c_str() << "\n";
	    assert(false);
	}

	std::cout << "loading timestamp keys\n";
	std::ifstream keyFile(key_path);
	std::vector<uint64_t> keys;

	uint64_t key = 0;
	for (uint64_t i = 0; i < key_count; i++) {
	    keyFile >> key;
	    keys.push_back(key);
	}
    keyFile.close();

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
	//     rocksdb::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
	//     setValueBuffer(value_buf, value_size, e, dist);
	//     rocksdb::Slice s_value(value_buf, value_size);

    //     rocksdb::WriteOptions writeOpt;
    //     writeOpt.disableWAL = false;
	//     status = (*db)->Put(rocksdb::WriteOptions(), s_key, s_value);
	//     if (!status.ok()) {
	// 	std::cout << status.ToString().c_str() << "\n";
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

	//std::cout << "compacting\n";
	//rocksdb::CompactRangeOptions compact_range_options;
	//(*db)->CompactRange(compact_range_options, NULL, NULL);
    }
    // int cnt = 0;
    // rocksdb::Iterator* it = (*db)->NewIterator(rocksdb::ReadOptions());
    // for (it->SeekToFirst(); it->Valid(); it->Next()) { 
    // //   std::cout << "key size: " << it->key().size() << " value size: " << it->value().size() <<"\n"; 
    //   cnt++; 
    // }
    // std::cout << "RocksDB total keys:  " << cnt << std::endl;
}

void close(rocksdb::DB* db) {
    delete db;
}

void testScan(const std::string& key_path, rocksdb::DB* db, uint64_t key_count) {
    std::cout << "testScan: loading timestamp keys\n";
    std::ifstream keyFile(key_path);
    std::vector<uint64_t> keys;

    uint64_t key = 0;
    for (uint64_t i = 0; i < key_count; i++) {
	keyFile >> key;
	keys.push_back(key);
    }
    keyFile.close();
    
    struct timespec ts_start;
    struct timespec ts_end;
    uint64_t elapsed;

    clock_gettime(CLOCK_MONOTONIC, &ts_start);

    for (uint64_t i = 0; i < key_count; i++) {
	key = htobe64(keys[i]);

	rocksdb::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
	std::string s_value;
	uint64_t value;

	rocksdb::Status status = db->Get(rocksdb::ReadOptions(), s_key, &s_value);

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

void warmup(const std::string key_path, uint64_t key_count, uint64_t sample_gap, rocksdb::DB* db) {
    std::ifstream keyFile(key_path);
    std::vector<uint64_t> keys;
    uint64_t key = 0;
    for (uint64_t i = 0; i < key_count; i++) {
	keyFile >> key;
	if (i % sample_gap == 0)
	    keys.push_back(key);
    }
    keyFile.close();
    
    struct timespec ts_start;
    struct timespec ts_end;
    //uint64_t elapsed;

    //std::cout << "warming up\n";
    //clock_gettime(CLOCK_MONOTONIC, &ts_start);

    for (uint64_t i = 0; i < keys.size(); i++) {
	key = keys[i];
	key = htobe64(key);

	rocksdb::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
	std::string s_value;
	uint64_t value;

	rocksdb::Status status = db->Get(rocksdb::ReadOptions(), s_key, &s_value);

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

void benchPointQuery(rocksdb::DB* db, rocksdb::Options* options, int seed,
		     uint64_t key_range, uint64_t query_count) {
    //std::random_device rd;
    //std::mt19937_64 e(rd());
    std::mt19937_64 e(seed);
    std::uniform_int_distribution<unsigned long long> dist(0, key_range);

    // struct timespec ts_start;
    // struct timespec ts_end;
    // uint64_t elapsed;

    // printf("point query\n");
    // clock_gettime(CLOCK_MONOTONIC, &ts_start);

    for (uint64_t i = 0; i < query_count; i++) {
	uint64_t key = dist(e);
	key = htobe64(key);

	rocksdb::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
	std::string s_value;
	uint64_t value;

	rocksdb::Status status = db->Get(rocksdb::ReadOptions(), s_key, &s_value);

	if (status.ok()) {
	    assert(s_value.size() >= sizeof(uint64_t));
	    value = *reinterpret_cast<const uint64_t*>(s_value.data());
	    (void)value;
	}
    }
    
    // clock_gettime(CLOCK_MONOTONIC, &ts_end);
    // elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
	// static_cast<uint64_t>(ts_end.tv_nsec) -
	// static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
	// static_cast<uint64_t>(ts_start.tv_nsec);

    // std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
    // std::cout << "throughput: " << (static_cast<double>(query_count) / (static_cast<double>(elapsed) / 1000000000.)) << " ops/sec\n";

    // std::string stats = options->statistics->ToString();
    // size_t pos = stats.find("rocksdb.db.get.micros statistics Percentiles");
    // size_t end_pos = stats.find("rocksdb.db.write.micros statistics Percentiles");
    // std::string latencies = stats.substr(pos, (end_pos - pos));
    // std::cout << latencies;
}

void benchOpenRangeQuery(rocksdb::DB* db, rocksdb::Options* options, int seed, uint64_t key_range,
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
    rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());

	uint64_t key = dist(e);
	key = htobe64(key);
	rocksdb::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
	
	std::string s_value;
	uint64_t value;

	uint64_t j = 0;
	for (it->Seek(s_key); it->Valid() && j < scan_length; it->Next(), j++) {
	    uint64_t found_key = *reinterpret_cast<const uint64_t*>(it->key().data());
	    assert(it->value().size() >= sizeof(uint64_t));
	    value = *reinterpret_cast<const uint64_t*>(it->value().data());
	    //(void)value;
	    //break;
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

    // std::string stats = options->statistics->ToString();
    // size_t pos = stats.find("rocksdb.db.seek.micros statistics Percentiles");
    // size_t end_pos = stats.find("rocksdb.db.write.stall statistics Percentiles");
    // std::string latencies = stats.substr(pos, (end_pos - pos));
    // std::cout << latencies;

}

void benchClosedRangeQuery(rocksdb::DB* db, rocksdb::Options* options, int seed, uint64_t key_range,
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
	rocksdb::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
	upper_key = htobe64(upper_key);
	rocksdb::Slice s_upper_key(reinterpret_cast<const char*>(&upper_key), sizeof(upper_key));
	
	std::string s_value;
	uint64_t value;

	rocksdb::ReadOptions read_options = rocksdb::ReadOptions();
	read_options.iterate_upper_bound = &s_upper_key;
	rocksdb::Iterator* it = db->NewIterator(read_options);
	uint64_t j = 0;
	for (it->Seek(s_key); it->Valid(); it->Next(), j++) {
	    uint64_t found_key = *reinterpret_cast<const uint64_t*>(it->key().data());
	    // assert(it->value().size() >= sizeof(uint64_t));
	    value = *reinterpret_cast<const uint64_t*>(it->value().data());
	    // (void)value;
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

    // std::string stats = options->statistics->ToString();
    // size_t pos = stats.find("rocksdb.db.seek.micros statistics Percentiles");
    // size_t end_pos = stats.find("rocksdb.db.write.stall statistics Percentiles");
    // std::string latencies = stats.substr(pos, (end_pos - pos));
    // std::cout << latencies;
}

void printIO() {
    FILE* fp = fopen("/sys/block/sda/sda2/stat", "r");
    if (fp == NULL) {
	printf("Error: empty fp\n");
	printf("%s\n", strerror(errno));
	return;
    }
    char buf[4096];
    if (fgets(buf, sizeof(buf), fp) != NULL)
	printf("%s", buf);
    fclose(fp);
    printf("\n");
}

uint64_t getIOCount() {
    std::ifstream io_file(std::string("/sys/block/sda/sda2/stat"));
    uint64_t io_count = 0;
    io_file >> io_count;
    return io_count;
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
    if (argc < 11) {
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
	std::cout << "\t0: init\n";
	std::cout << "\t1: point query\n";
	std::cout << "\t2: open range query\n";
	std::cout << "\t3: closed range query\n";
	std::cout << "arg 6: range size\n";
	std::cout << "arg 7: warmup # of queries\n";
	std::cout << "arg 8: total # of keys\n";
	std::cout << "arg 9: total # of queries\n";
	std::cout << "arg 10: # of threads\n";
	std::cout << "arg 11: cache all index\n";
	return -1;
    }

    std::string db_path = std::string(argv[1]);
    int filter_type = atoi(argv[2]);
    int compression_type = atoi(argv[3]);
    int block_cache_size = atoi(argv[4]);
    int query_type = atoi(argv[5]);
    uint64_t range_size = (uint64_t)atoi(argv[6]);
    uint64_t warmup_query_count = (uint64_t)atoi(argv[7]);
    int thread_cnt = atoi(argv[10]);
    int index_type = atoi(argv[11]);
    uint64_t scan_length = 10;
    scan_length = range_size;

    const std::string kKeyPath = "poisson_timestamps.csv";
    const uint64_t kValueSize = 1009;
    const uint64_t kKeyRange = 50000000000000;
    uint64_t kQueryCount = 50000;
    kQueryCount = (uint64_t)atoi(argv[9]);
    
    // 2GB config
    // const uint64_t kKeyCount = 2000000;
    // const uint64_t kWarmupSampleGap = 100;

    // 500GB config
    uint64_t kKeyCount = 500000000;
    kKeyCount = (uint64_t)atoi(argv[8]);
    const uint64_t kWarmupSampleGap = kKeyCount / warmup_query_count;

    // 100GB config
    //const uint64_t kKeyCount = 100000000;
    //const uint64_t kWarmupSampleGap = kKeyCount / warmup_query_count;

    //=========================================================================
    
    rocksdb::DB* db;
    rocksdb::Options options;
    rocksdb::BlockBasedTableOptions table_options;

    init(kKeyPath, db_path, &db, &options, &table_options, block_cache_size, kKeyCount, kValueSize, filter_type, compression_type, thread_cnt, index_type);

    if (query_type == 0){
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

    uint64_t io_before = getIOCount();
    //mem_free_before = getMemFree();
    //mem_available_before = getMemAvailable();
    
    // if (query_type == 1)
	// benchPointQuery(db, &options, kKeyRange, kQueryCount);
    // else if (query_type == 2)
	// benchOpenRangeQuery(db, &options, kKeyRange, kQueryCount, scan_length);
    // else if (query_type == 3)
	// benchClosedRangeQuery(db, &options, kKeyRange, kQueryCount, range_size);

    struct timespec ts_start;
    struct timespec ts_end;
    uint64_t elapsed;
    
    clock_gettime(CLOCK_MONOTONIC, &ts_start);

    std::thread *thrd[MAX_THREAD_CNT];
    for (int i = 0; i< thread_cnt; i++) {
        if (query_type == 1)
        thrd[i] = new std::thread(benchPointQuery, db, &options, i, kKeyRange, kQueryCount);
        else if (query_type == 2)
        thrd[i] = new std::thread(benchOpenRangeQuery, db, &options, i, kKeyRange, kQueryCount, scan_length);
        else if (query_type == 3)
        thrd[i] = new std::thread(benchClosedRangeQuery,db, &options, i, kKeyRange, kQueryCount, range_size);
        else 
        thrd[i] = new std::thread(benchPointQuery, db, &options, i, kKeyRange, kQueryCount);
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

    uint64_t io_after = getIOCount();
    //mem_free_after = getMemFree();
    //mem_available_after = getMemAvailable();
    //std::cout << options.statistics->ToString() << "\n";
    //std::string stats;
    //db->GetProperty(rocksdb::Slice("rocksdb.stats"), &stats);
    //std::cout << stats << "\n";
    //printIO();

    std::cout << "I/O count: " << (io_after - io_before) << "\n";


    close(db);

    return 0;
}