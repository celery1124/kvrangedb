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
#include <iomanip>
#include <sstream>
#include <mutex>
#include <algorithm>

#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"

//#include "rocksdb/utilities/leveldb_options.h"


#define FNV_OFFSET_BASIS_64  0xCBF29CE484222325
#define FNV_OFFSET_BASIS_32  0x811c9dc5
#define FNV_PRIME_64  1099511628211L
#define FNV_PRIME_32 16777619
#define MAX_THREAD_CNT 64

using namespace rocksdb;

std::vector<ColumnFamilyHandle*> handles;

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

void init(const std::string& db_path, rocksdb::DB** db, std::vector<ColumnFamilyHandle*>& handles,
	  rocksdb::Options* options, rocksdb::BlockBasedTableOptions* table_options,
	  uint64_t block_cache_size, int filter_type, int compression_type, int index_type) {

    if (filter_type == 1)
	table_options->filter_policy.reset(rocksdb::NewBloomFilterPolicy(14, false));
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
    table_options->block_cache = rocksdb::NewLRUCache(block_cache_size*1024*1024);
    std::cout << "Block cache size: " <<  block_cache_size << "MB\n";

    table_options->pin_l0_filter_and_index_blocks_in_cache = true;  
    if (index_type == 0) {
        printf("fixed block cache size %d MB\n", block_cache_size);
        table_options->cache_index_and_filter_blocks = true;
    }
    else if (index_type == 1) {
        printf("cache all index/filter blocks\n");
        table_options->cache_index_and_filter_blocks = false;
        options->max_open_files = -1;
    }
    else {
        table_options->cache_index_and_filter_blocks = true;
    }

    options->table_factory.reset(rocksdb::NewBlockBasedTableFactory(*table_options));

    // options->max_open_files = -1; // pre-load indexes and filters

    // 2GB config
    // options->write_buffer_size = 2 * 1048576;
    // options->max_bytes_for_level_base = 10 * 1048576;
    // options->target_file_size_base = 2 * 1048576;

    // 100GB config
    //options->write_buffer_size = 64 * 1048576;
    //options->max_bytes_for_level_base = 256 * 1048576;
    //options->target_file_size_base = 64 * 1048576;

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

    // open DB with two column families
    ColumnFamilyOptions cfoptions;
    cfoptions.table_factory.reset(rocksdb::NewBlockBasedTableFactory(*table_options));
    
    std::vector<ColumnFamilyDescriptor> column_families;
    // have to open default column family
    column_families.push_back(ColumnFamilyDescriptor(
        kDefaultColumnFamilyName, cfoptions));
    // open the new one, too
    column_families.push_back(ColumnFamilyDescriptor(
        "t", cfoptions));
    rocksdb::Status status = DB::Open(*options, db_path, column_families, &handles, db);
    //rocksdb::Status status = rocksdb::DB::Open(*options, db_path, db);
    if (!status.ok()) {
        std::string err = status.ToString();
        std::cout << err << "\n";
    }
    assert(status.ok());

}

void benchInsert(rocksdb::DB* db, rocksdb::Options* options,
		    uint64_t start, uint64_t query_count, int val_size) {

    RandomGenerator gen;
    Random rand(start);

    for (uint64_t i = start; i < start+query_count; i++) {
	uint64_t r = fnvhash64(i);
	std::string str_key = buildKey(16, r);
	std::string str_value;
    buildVal(gen, val_size, str_value);
    rocksdb::Slice s_key(str_key);
    rocksdb::Slice s_value(str_value);

    rocksdb::Status status = db->Put(rocksdb::WriteOptions(), handles[0], s_key, s_value);
    }

}

void load(const std::string& db_path, rocksdb::DB** db,
	  rocksdb::Options* options, rocksdb::BlockBasedTableOptions* table_options,
	  int filter_type, int compression_type, int thread_cnt, int kInsertCount, int val_size) {
    
    if (filter_type == 1)
	table_options->filter_policy.reset(rocksdb::NewBloomFilterPolicy(14, false));
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

    // table_options->pin_l0_filter_and_index_blocks_in_cache = true;
    table_options->cache_index_and_filter_blocks = true;

    options->table_factory.reset(rocksdb::NewBlockBasedTableFactory(*table_options));

    // options->max_open_files = -1; // pre-load indexes and filters

    // 2GB config
    // options->write_buffer_size = 2 * 1048576;
    // options->max_bytes_for_level_base = 10 * 1048576;
    // options->target_file_size_base = 2 * 1048576;

    // 100GB config
    options->write_buffer_size = 64 * 1048576;
    options->max_bytes_for_level_base = 256 * 1048576;
    options->target_file_size_base = 64 * 1048576;
    options->statistics = rocksdb::CreateDBStatistics();

	options->allow_mmap_reads = false;
	options->allow_mmap_writes = false;
	options->use_direct_reads = true;
    options->use_direct_io_for_flush_and_compaction=true;

    // options->statistics = rocksdb::CreateDBStatistics();
    options->IncreaseParallelism(32);
    options->OptimizeLevelStyleCompaction();

    options->create_if_missing = true;
    //options->error_if_exists = false;
    
    //options->prefix_extractor = nullptr;
    //options->disable_auto_compactions = false;
  
    rocksdb::Status status = rocksdb::DB::Open(*options, db_path, db);
    if (!status.ok()) {
        std::string err = status.ToString();
        std::cout << err << "\n";
    }
    // create column family
    ColumnFamilyHandle* cf;
    status = (*db)->CreateColumnFamily(ColumnFamilyOptions(), "t", &cf);
    assert(status.ok());

    handles.push_back(cf);

    std::thread *thrd[MAX_THREAD_CNT];
    for (int i = 0; i< thread_cnt; i++) {
        thrd[i] = new std::thread(benchInsert, *db, options, kInsertCount*i, kInsertCount, val_size);
    }
    for (int i = 0; i< thread_cnt; i++) {
        thrd[i]->join();
    }

}


void close(rocksdb::DB* db, std::vector<ColumnFamilyHandle*>& handles) {
    for (auto handle : handles) {
        Status s = db->DestroyColumnFamilyHandle(handle);
        assert(s.ok());
    }
    delete db;
}

void warmup(uint64_t key_count, rocksdb::DB* db, std::vector<ColumnFamilyHandle*>& handles) {
    
    
    struct timespec ts_start;
    struct timespec ts_end;
    uint64_t elapsed;

    std::cout << "warming up\n";
    clock_gettime(CLOCK_MONOTONIC, &ts_start);

    for (uint64_t i = 0; i < key_count; i++) {
	uint64_t key = fnvhash64(i+1000000000);
	std::string str_key = buildKey(16, key);


	rocksdb::Slice s_key(str_key);
	std::string s_value;

	rocksdb::Status status = db->Get(rocksdb::ReadOptions(), handles[1], s_key, &s_value);

	if (status.ok()) {
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


void benchPointQuery(rocksdb::DB* db, rocksdb::Options* options,
		     uint64_t start, uint64_t query_count) {

    // printf("point query\n");

    for (uint64_t i = start; i < query_count+start; i++) {
	uint64_t key = fnvhash64(i+1000000000);
	std::string str_key = buildKey(16, key);

	rocksdb::Slice s_key(str_key);
	std::string s_value;

	rocksdb::Status status = db->Get(rocksdb::ReadOptions(), handles[1], s_key, &s_value);

	if (status.ok()) {
        // std::cout << "key: " << str_key << " found\n";
	    // printf("value size: %d\n", s_value.size());
        // for (int i = 0; i < 14; i++) {
        //     printf("%d(%c) ",s_value[i], s_value[i]);
        // }
        // printf("\n");
	}
    else {
        // std::cout << "key: " << str_key << " not found\n";
    }
    }
    
    

    // std::string stats = options->statistics->ToString();
    // size_t pos = stats.find("rocksdb.db.get.micros statistics Percentiles");
    // size_t end_pos = stats.find("rocksdb.db.write.micros statistics Percentiles");
    // std::string latencies = stats.substr(pos, (end_pos - pos));
    // std::cout << latencies;
}

void benchOpenRangeQuery(rocksdb::DB* db, rocksdb::Options* options, uint64_t key_range,
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
    rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());

    clock_gettime(CLOCK_MONOTONIC, &ts_start);

    for (uint64_t i = 0; i < query_count; i++) {
	uint64_t key = query_keys[i];
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
    }
    
    clock_gettime(CLOCK_MONOTONIC, &ts_end);
    elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_end.tv_nsec) -
	static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_start.tv_nsec);

    std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
    std::cout << "throughput: " << (static_cast<double>(query_count) / (static_cast<double>(elapsed) / 1000000000.)) << " ops/sec\n";

    std::string stats = options->statistics->ToString();
    size_t pos = stats.find("rocksdb.db.seek.micros statistics Percentiles");
    size_t end_pos = stats.find("rocksdb.db.write.stall statistics Percentiles");
    std::string latencies = stats.substr(pos, (end_pos - pos));
    std::cout << latencies;

    delete it;
}

void benchClosedRangeQuery(rocksdb::DB* db, rocksdb::Options* options, uint64_t start,
			   uint64_t query_count, uint64_t range_size) {

   
    // printf("closed range query\n");

    int closed_seek = 0;

    for (uint64_t i = start; i < query_count+start; i++) {
	uint64_t key = fnvhash64(i+1000000000);
    std::string str_key = buildKey(16, key);
    std::string str_upper_key (str_key);
    std::reverse(str_upper_key.begin(), str_upper_key.end());

	rocksdb::Slice s_key(str_key);

	uint32_t *upper_key = (uint32_t *)str_upper_key.data() ;
    *upper_key = *upper_key + range_size;
    std::reverse(str_upper_key.begin(), str_upper_key.end());
    
	rocksdb::Slice s_upper_key(str_upper_key);
	

	rocksdb::ReadOptions read_options = rocksdb::ReadOptions();
	read_options.iterate_upper_bound = &s_upper_key;
	rocksdb::Iterator* it = db->NewIterator(read_options, handles[1]);
	uint64_t j = 0;
    // std::cout << "seek from " << str_key << " to " << str_upper_key << "\n";
	for (it->Seek(s_key); it->Valid() && j<10; it->Next(), j++) {
	    rocksdb::Slice found_key = it->key();
        closed_seek++;
        break;
	}
	
	delete it;
    }

    printf("closed seek %d/%d\n", closed_seek, query_count);
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
    std::ifstream io_file(std::string("/sys/block/nvme2n1/stat"));
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
    if (argc < 10) {
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
	std::cout << "arg 4: block cache size\n";
	std::cout << "\t0: no\n";
	std::cout << "\t1: yes\n";
	std::cout << "arg 5: query type\n";
	std::cout << "\t0: init\n";
	std::cout << "\t1: point query\n";
	std::cout << "\t2: open range query\n";
	std::cout << "\t3: closed range query\n";
	std::cout << "arg 6: range size\n";
	std::cout << "arg 7: warmup # of queries\n";
	std::cout << "arg 8: # of queries\n";
	std::cout << "arg 9: # of threads\n";
	std::cout << "arg 10: value size\n";
	std::cout << "arg 11: index type\n";
	std::cout << "\t0: share block cache\n";
	std::cout << "\t1: cache all index/filter blocks\n";
	return -1;
    }
    
    const uint64_t kKeyRange = 10000000000000;
    uint64_t kQueryCount = 50000;
    uint64_t warmup_query_count = 1000000;

    std::string db_path = std::string(argv[1]);
    int filter_type = atoi(argv[2]);
    int compression_type = atoi(argv[3]);
    uint64_t block_cache_size = atoi(argv[4]);
    int query_type = atoi(argv[5]);
    uint64_t range_size = (uint64_t)atoi(argv[6]);
    warmup_query_count = (uint64_t)atoi(argv[7]);
    kQueryCount = (uint64_t)atoi(argv[8]);
    int thread_cnt = atoi(argv[9]);
    int val_size = atoi(argv[10]);
    int index_type = atoi(argv[11]);
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
    
    rocksdb::DB* db;
    rocksdb::Options options;
    rocksdb::BlockBasedTableOptions table_options;

    if (query_type == 0){
        struct timespec ts_start;
        struct timespec ts_end;
        uint64_t elapsed;
        printf("Load database, #keys %d\n", thread_cnt*kQueryCount);
        clock_gettime(CLOCK_MONOTONIC, &ts_start);

        load(db_path, &db, &options, &table_options, filter_type, compression_type, thread_cnt, kQueryCount, val_size);

        clock_gettime(CLOCK_MONOTONIC, &ts_end);
        elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
        static_cast<uint64_t>(ts_end.tv_nsec) -
        static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
        static_cast<uint64_t>(ts_start.tv_nsec);

        std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
        std::cout << "throughput: " << (static_cast<double>(kQueryCount*thread_cnt) / (static_cast<double>(elapsed) / 1000000000.)) << " ops/sec\n";

        std::string stats = options.statistics->ToString();
        size_t pos = stats.find("rocksdb.db.write.micros statistics Percentiles");
        size_t end_pos = stats.find("rocksdb.compaction.times.micros statistics Percentiles");
        std::string latencies = stats.substr(pos, (end_pos - pos));
        std::cout << latencies;

        close(db, handles);
        return 0;
    }
	
    init(db_path, &db, handles, &options, &table_options, block_cache_size, filter_type, compression_type, index_type);

    //=========================================================================

    uint64_t mem_free_before = getMemFree();
    uint64_t mem_available_before = getMemAvailable();
    //std::cout << options.statistics->ToString() << "\n";
    //printIO();
    warmup(warmup_query_count, db, handles);
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
    
    clock_gettime(CLOCK_MONOTONIC, &ts_start);

    std::thread *thrd[MAX_THREAD_CNT];
    for (int i = 0; i< thread_cnt; i++) {
        if (query_type == 1)
        thrd[i] = new std::thread(benchPointQuery, db, &options, i*kQueryCount, kQueryCount);
        else if (query_type == 2)
        thrd[i] = new std::thread(benchOpenRangeQuery, db, &options, i*kQueryCount, kQueryCount, scan_length);
        else if (query_type == 3)
        thrd[i] = new std::thread(benchClosedRangeQuery,db, &options, i*kQueryCount, kQueryCount, range_size);
        else 
        thrd[i] = new std::thread(benchPointQuery, db, &options, i*kQueryCount, kQueryCount);
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
    //std::cout << "Mem Free diff: " << (mem_free_before - mem_free_after) << "\n";
    //std::cout << "Mem Aavilable diff: " << (mem_available_before - mem_available_after) << "\n";

    // report latency

    std::string stats = options.statistics->ToString();
    size_t pos = stats.find("rocksdb.db.get.micros statistics Percentiles");
    size_t end_pos = stats.find("rocksdb.db.write.micros statistics Percentiles");
    std::string latencies = stats.substr(pos, (end_pos - pos));
    std::cout << latencies << "\n";
    pos = stats.find("rocksdb.bloom.filter.useful COUNT");
    end_pos = stats.find("rocksdb.persistent.cache.hit COUNT");
    std::string get_filter = stats.substr(pos, (end_pos - pos));
    std::cout << get_filter << "\n";
    
    io_before = getIOCount();
    clock_gettime(CLOCK_MONOTONIC, &ts_start);

    std::cout << "range distance: " << range_size << "\n";
    std::thread *thrd2[MAX_THREAD_CNT];
    for (int i = 0; i< thread_cnt; i++) {
        // if (query_type == 1)
        // thrd2[i] = new std::thread(benchPointQuery, db, &options, i*kQueryCount, kQueryCount);
        // else if (query_type == 2)
        // thrd2[i] = new std::thread(benchOpenRangeQuery, db, &options, i*kQueryCount, kQueryCount, scan_length);
        // else if (query_type == 3)
        thrd2[i] = new std::thread(benchClosedRangeQuery,db, &options, i*kQueryCount, kQueryCount, range_size);
        // else 
        // thrd2[i] = new std::thread(benchPointQuery, db, &options, i*kQueryCount, kQueryCount);
    }
    for (int i = 0; i< thread_cnt; i++) {
        thrd2[i]->join();
    }

    clock_gettime(CLOCK_MONOTONIC, &ts_end);
    elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_end.tv_nsec) -
	static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_start.tv_nsec);

    std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
    std::cout << "throughput: " << (static_cast<double>(kQueryCount*thread_cnt) / (static_cast<double>(elapsed) / 1000000000.)) << " ops/sec\n";

    std::cout << "I/O count: " << (io_before - io_after) << "\n";

    std::string stats2 = options.statistics->ToString();
    pos = stats2.find("rocksdb.db.seek.micros statistics Percentiles");
    end_pos = stats2.find("rocksdb.db.write.stall statistics Percentiles");
    std::cout << stats2.substr(pos, (end_pos - pos)) << "\n";
    pos = stats.find("rocksdb.bloom.filter.useful COUNT");
    end_pos = stats.find("rocksdb.persistent.cache.hit COUNT");
    std::string seek_filter = stats.substr(pos, (end_pos - pos));
    std::cout << seek_filter << "\n";

    close(db, handles);

    return 0;
}