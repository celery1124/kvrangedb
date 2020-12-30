
/* db_perf.cc
* 11/11/2020
* by Mian Qin
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <new>
#include <assert.h>
#include <unistd.h>
#include <thread>
#include <unordered_map>
#include <ctime>
#include <chrono>
#include <iostream>
#include <mutex>
#include <atomic>

#include "kvrangedb/slice.h"
#include "kvrangedb/comparator.h"
#include "kvrangedb/db.h"

#define PERF_RD_LAT 1

#define SUCCESS 0
#define FAILED 1

#define WRITE_OP  1
#define READ_OP   2
#define DELETE_OP 3
#define ITERATOR_OP 4
#define KEY_EXIST_OP 5

#define MILLION 1000000
#define ACCUM_GRANU 10
#define STATS_POOL_INT 10

static int key_gen = 0;
static int hint_packed = 0;

struct thread_args{
  int id;
  uint8_t klen;
  uint32_t vlen;
  int count;
  int op_type;
  char *keys;
  int rd_nums;
  int key_mode;
  int key_offset;
  std::unordered_map<int, int> rd_lat;
  kvrangedb::DB *db;
};

void usage(char *program)
{
  printf("==============\n");
  printf("usage: %s -d device_path [-n num_ios] [-o op_type] [-k klen] [-v vlen] [-t threads]\n", program);
  printf("-d      device_path  :  kvssd device path. e.g. emul: /dev/kvemul; kdd: /dev/nvme0n1; udd: 0000:06:00.0\n");
  printf("-n      num_ios      :  total number of ios (ignore this for iterator)\n");
  printf("-o      op_type      :  1: write; 2: read; 3: delete; 4: iterator; 5: key exist check\n");
  printf("-k      klen         :  key length (ignore this for iterator)\n");
  printf("-v      vlen         :  value length (ignore this for iterator)\n");
  printf("-t      threads      :  number of threads\n");
  printf("==============\n");
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


const long FNV_OFFSET_BASIS_64 = 0xCBF29CE484222325L;
const long FNV_PRIME_64 = 1099511628211L;
long fnvhash64(long val) {
  //from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
  long hashval = FNV_OFFSET_BASIS_64;

  for (int i = 0; i < 8; i++) {
    long octet = val & 0x00ff;
    val = val >> 8;

    hashval = hashval ^ octet;
    hashval = hashval * FNV_PRIME_64;
    //hashval = hashval ^ octet;
  }
  return hashval;
}


class CustomComparator : public kvrangedb::Comparator {
public:
  CustomComparator() {}
  ~CustomComparator() {}
  int Compare(const kvrangedb::Slice& a, const kvrangedb::Slice& b) const {
    return a.compare(b);
  }
};

std::atomic<bool> stats_end (false);
std::atomic<uint64_t> ops_keys (0);
void stats_thread(int total_ops, int pool_interval_milisec, int print_interval_million, int print_interval_sec, int mode /*0-by record, 1-by time*/) {
  const auto timeWindow = std::chrono::milliseconds(pool_interval_milisec);
  const auto printTimeWindow = std::chrono::seconds(print_interval_sec);
  uint64_t prev_keys = 0;
  auto prev_ts = std::chrono::system_clock::now();
  if (mode == 0) {
    while(stats_end.load(std::memory_order_relaxed) == false)
    {
        // print when exceed 
        uint64_t curr_keys = ops_keys.load(std::memory_order_relaxed);
        if (curr_keys - prev_keys/MILLION*MILLION >= MILLION) {
          auto curr_ts = std::chrono::system_clock::now();
          std::chrono::duration<double> wctduration = (curr_ts - prev_ts);
          fprintf(stderr, "[%.3f sec] Throughput %.6f ops/sec, current keys %lu\n",wctduration.count(), (double)(curr_keys-prev_keys)/wctduration.count(), curr_keys);
          prev_ts = curr_ts;
          prev_keys = curr_keys;
        }

        // sleep
        std::this_thread::sleep_for(timeWindow);
    }
  }
  else if (mode == 1) {
    while(stats_end.load(std::memory_order_relaxed) == false)
    {
        // sleep
        std::this_thread::sleep_for(printTimeWindow);
        if (stats_end.load(std::memory_order_relaxed) == true) break;

        // print when exceed 
        auto curr_ts = std::chrono::system_clock::now();
        std::chrono::duration<double> wctduration = (curr_ts - prev_ts);
        uint64_t curr_keys = ops_keys.load(std::memory_order_relaxed);
        double curr_tps = (double)(curr_keys-prev_keys)/print_interval_sec;
        fprintf(stderr, "[%.3f sec] Throughput %.6f ops/sec, current keys %lu, remain time (%.3f minutes)\n",wctduration.count(), curr_tps, curr_keys, (double)(total_ops-curr_keys)/curr_tps/60);
        prev_ts = curr_ts;
        prev_keys = curr_keys;
    }
  }
}

int perform_read(int id, kvrangedb::DB *db, int count, uint8_t klen, uint32_t vlen, char *keys, int key_mode, int rd_nums, std::unordered_map<int, int>& rd_lat) {
  kvrangedb::ReadOptions rdopts;
  rdopts.hint_packed = hint_packed;
  Random rdn(id);
  int ret;
  char *key   = (char*)malloc(klen);
  char *value = (char*)malloc(vlen);
  if(key == NULL || value == NULL) {
    fprintf(stderr, "failed to allocate\n");
    return FAILED;
  }
  struct timespec t1, t2;

  int start_key = id * count;
  for (int i = start_key; i < start_key + rd_nums; i++) {
    //int seed = rdn.Next()%count;
    int seed = i%100000;
    memset(value, 0, vlen);
    //sprintf(key, "%0*d", klen - 1, i);
    if (key_gen == 1) {
      memcpy(key, keys + klen*seed, klen);
    }
    else {
      if (key_mode == 0) {
        char *key_str = (char*) malloc (sizeof(long)*2+1);
        long hash_key = fnvhash64(start_key+seed);
        sprintf(key_str, "%0*lX", (int)sizeof(long)*2 , hash_key);

        for (int jj=0; jj<klen; jj++) {
          memcpy(key+jj, &key_str[jj%(sizeof(long)*2)], 1);
        }
        free(key_str);
      }
      else if (key_mode == 1) {
        char *tmp = (char*)malloc(klen+1);
        sprintf(tmp, "%0*x", klen , start_key+seed);
        memcpy(key, tmp, klen);
        free(tmp);
      }
      else if (key_mode == 2) {
        int idx = start_key+seed;
        memset(key, 0, klen);
        memcpy(key, (char *)&idx, sizeof(int));
      }
    }

    kvrangedb::Slice db_key(key, klen);
    std::string val;

#ifdef PERF_RD_LAT
    clock_gettime(CLOCK_REALTIME, &t1);
#endif

    kvrangedb::Status ret = db->Get(rdopts, db_key, &val);;

#ifdef PERF_RD_LAT
    clock_gettime(CLOCK_REALTIME, &t2);
    rd_lat[start_key+seed] = (t2.tv_sec-t1.tv_sec)*1000000 + (t2.tv_nsec-t1.tv_nsec)/1000;
#endif

    if( !ret.ok() ) {
      fprintf(stderr, "retrieve tuple %s (%d) failed with error\n", std::string(key, klen).c_str(), seed+start_key);
      //exit(1);
    } else {
      //fprintf(stdout, "retrieve tuple %s with value = %s, vlen = %d \n", key, std::string(val.data(), 8).c_str(), val.size());
    }

    if (i%ACCUM_GRANU == (ACCUM_GRANU-1)) {
      ops_keys.fetch_add(ACCUM_GRANU, std::memory_order_relaxed);
    }
  }

  if(key) free(key);
  if(value) free(value);
  
  return SUCCESS;
}


int perform_insertion(int id, kvrangedb::DB *db, int count, uint8_t klen, uint32_t vlen, char *keys, int key_mode, int key_offset) {
  kvrangedb::WriteOptions wropts;
  RandomGenerator gen;
  char *key   = (char*)malloc(klen);
  char *value = (char*)malloc(vlen);
  if(key == NULL || value == NULL) {
    fprintf(stderr, "failed to allocate\n");
    return FAILED;
  }

  int start_key = id * count + key_offset;

  for(int i = start_key; i < start_key + count; i++) {
    
    if (key_gen == 1) {
      memcpy(key, keys, klen);
    }
    else {
      if (key_mode == 0) {
        char *key_str = (char*) malloc (sizeof(long)*2+1);
        long hash_key = fnvhash64(i);
        sprintf(key_str, "%0*lX", (int)sizeof(long)*2 , hash_key);

        for (int jj=0; jj<klen; jj++) {
          memcpy(key+jj, &key_str[jj%(sizeof(long)*2)], 1);
        }
        free(key_str);
      }
      else if (key_mode == 1) {
        char *tmp = (char*)malloc(klen+1);
        sprintf(tmp, "%0*x", klen , i);
        memcpy(key, tmp, klen);
        free(tmp);
      }
      else if (key_mode == 2) {
        memset(key, 0, klen);
        memcpy(key, (char *)&i, sizeof(int));
      }
      
    }
    char *rand_val = gen.Generate(vlen);
    memcpy(value, rand_val, vlen);
    
    kvrangedb::Slice db_key(key, klen);
    kvrangedb::Slice db_val(value, vlen);

    kvrangedb::Status ret = db->Put(wropts, db_key, db_val);
    if( !ret.ok() ) {
      fprintf(stderr, "store tuple %s (%d) failed with error \n", std::string(key, klen).c_str(), i);
      free(key);
      free(value);
      return FAILED;
    } else {
      //fprintf(stdout, "thread %d store key %s with value %s done \n", id, key, value);
    }

    keys += klen;

    if (i%ACCUM_GRANU == (ACCUM_GRANU-1)) {
      ops_keys.fetch_add(ACCUM_GRANU, std::memory_order_relaxed);
    }
  }

  if(key) free(key);
  if(value) free(value);

  return SUCCESS;
}


void do_io(int id, kvrangedb::DB *db, int count, uint8_t klen, uint32_t vlen, int op_type, char *keys, int rd_nums, int key_mode, int key_offset, std::unordered_map<int, int>& rd_lat) {

  switch(op_type) {
  case WRITE_OP:
    perform_insertion(id, db, count, klen, vlen, keys, key_mode, key_offset);
    break;
  case READ_OP:
    //perform_insertion(id, cont_hd, count, klen, vlen);
    perform_read(id, db, count, klen, vlen, keys, key_mode, rd_nums, rd_lat);
    break;
  // case DELETE_OP:
  //   //perform_insertion(id, cont_hd, count, klen, vlen);
  //   perform_delete(id, cont_hd, count, klen, vlen);
  //   break;
  // case ITERATOR_OP:
  //   //perform_insertion(id, cont_hd, count, klen, vlen);
  //   perform_iterator(cont_hd);
  //   //Iterator a key-value pair only works in emulator
  //   //perform_iterator(cont_handle, 1);
  //   break;
  // case KEY_EXIST_OP:
  //   //perform_insertion(id, cont_hd, count, klen, vlen);
  //   perform_key_exist(id, cont_hd, count, klen, vlen);
  //   break;
  default:
    fprintf(stderr, "Please specify a correct op_type for testing\n");
    return;

  }
}

void *iothread(void *args)
{
  thread_args *targs = (thread_args *)args;
  do_io(targs->id, targs->db, targs->count, targs->klen, targs->vlen, targs->op_type, targs->keys, targs->rd_nums, targs->key_mode, targs->key_offset, targs->rd_lat);
  return 0;
}

void prepare_keys(int start, int count, uint8_t klen, int mode, char *&keys) {
  keys = (char *) calloc (1, (long)count * klen + 1);
  char *key = keys;
  for (int i = start; i < start+count; i++) {
    if (mode == 0) {
      char *key_str = (char*) malloc (sizeof(long)*2+1);
      long hash_key = fnvhash64(i);
      sprintf(key_str, "%0*lX", (int)sizeof(long)*2 , hash_key);

      for (int jj=0; jj<klen; jj++) {
        memcpy(key+jj, &key_str[jj%(sizeof(long)*2)], 1);
      }
      free(key_str);
    }
    else if (mode == 1) {
      sprintf(key, "%0*x", klen , i);
    }
    else if (mode == 2) {
      memcpy(key, (char *)&i, sizeof(int));
    }
    
    key = key + klen;
  }
}


#define HIST_NUM 50
#define HIST_INT 20 // us
#define HIST_START 50 // us
void proc_rd_lat(int t, thread_args *args) {
  int hist_lat[HIST_NUM] = {0}; // from HIST_START us to (HIST_NUM-1)*HIST_INT+HIST_START us
  int hist_key_cnt[HIST_NUM] = {0}; // 
  int hist_key[HIST_NUM] = {0}; // 
  for (int i = 0; i < t; i++) {
    for (auto it = args[i].rd_lat.begin(); it != args[i].rd_lat.end(); ++it) {
      int lat_reg = it->second;
      if (it->second < HIST_START) lat_reg = HIST_START;
      else if (it->second > (HIST_NUM-1)*HIST_INT+HIST_START) lat_reg = (HIST_NUM-1)*HIST_INT+HIST_START;
      
      hist_lat[(lat_reg-HIST_START)/HIST_INT]++;
      hist_key_cnt[(int)((double)(it->first)/(args->count * t)*HIST_NUM)]++;
      hist_key[(int)((double)(it->first)/(args->count * t)*HIST_NUM)] += it->second;
      
    }
  }
  printf("rd_lat_hist: ");
  for (int i = 0; i < HIST_NUM; i++) {
    printf("%d ", hist_lat[i]);
  }
  printf("\n");

  printf("key_lat_hist: ");
  for (int i = 0; i < HIST_NUM; i++) {
    printf("%.3f ", (double)hist_key[i]/hist_key_cnt[i]);
  }
  printf("\n");

}

int main(int argc, char *argv[]) {
  char* dev_path = NULL;
  int num_ios = 10;
  int num_rds = 100000;
  int op_type = 1;
  uint8_t klen = 16;
  uint32_t vlen = 4096;
  int ret, c, t = 1;
  int stats_mode = 1;
  int kmode = 0;
  int start_record = 0;
  int packThres = 4096;

  while ((c = getopt(argc, argv, "d:n:o:k:v:t:m:e:g:r:s:p:i:h")) != -1) {
    switch(c) {
    case 'd':
      dev_path = optarg;
      break;
    case 'n':
      num_ios = atoi(optarg);
      break;
    case 'o':
      op_type = atoi(optarg);
      break;
    case 'k':
      klen = atoi(optarg);
      break;
    case 'v':
      vlen = atoi(optarg);
      break;
    case 't':
      t = atoi(optarg);
      break;
    case 'm':
      stats_mode = atoi(optarg);
      break;
    case 'e':
      kmode = atoi(optarg);
      break;
    case 'g':
      key_gen = atoi(optarg);
      break;
    case 'r':
      num_rds = atoi(optarg);
      break;
    case 's':
      start_record = atoi(optarg);
      break;
    case 'p':
      packThres = atoi(optarg);
      break;
    case 'i':
      hint_packed = atoi(optarg);
      break;
    case 'h':
      usage(argv[0]);
      return SUCCESS;
    default:
      usage(argv[0]);
      return SUCCESS;
    }
  }

  if(dev_path == NULL) {
    fprintf(stderr, "Please specify KV SSD device path\n");
    usage(argv[0]);
    return SUCCESS;
  }

  if(op_type == ITERATOR_OP && t > 1) {
    fprintf(stdout, "Iterator only supports single thread \n");
    return FAILED;
  }
  
  CustomComparator cmp;
  kvrangedb::Options options;
  options.comparator = &cmp;
  options.indexNum = 1;
  options.indexType = kvrangedb::ROCKS;
  options.packThres = packThres;
  options.packThreadsNum = 12;
  options.statistics = kvrangedb::Options::CreateDBStatistics();

  kvrangedb::DB *db = NULL;
  kvrangedb::DB::Open(options, dev_path, &db);

  thread_args args[t];
  pthread_t tid[t];
  printf("dev: %s, klen: %d, vlen: %d, num_ios: %d, t: %d\n", dev_path, klen, vlen, num_ios, t);

  char *keys = nullptr;
  if (key_gen == 1) {
    prepare_keys(start_record, num_ios * t, klen, kmode, keys);
    printf("Prepare %lu %d bytes keys\n", (long)num_ios*t, klen);
  }

  struct timespec t1, t2;
  clock_gettime(CLOCK_REALTIME, &t1);
  int total_ops = num_ios*t;
  if (op_type == 1) total_ops = num_ios*t;
  else if (op_type == 2) total_ops = num_rds*t;
  std::thread stat_thread(stats_thread, total_ops, STATS_POOL_INT, 1, 10, stats_mode);

  for(int i = 0; i < t; i++){
    args[i].id = i;
    args[i].klen = klen;
    args[i].vlen = vlen;
    args[i].count = num_ios;
    args[i].db = db;
    args[i].op_type = op_type;
    args[i].keys = keys + (long)i*num_ios*klen;
    args[i].rd_nums = num_rds;
    args[i].key_mode = kmode;
    args[i].key_offset = start_record;
    pthread_attr_t *attr = (pthread_attr_t *)malloc(sizeof(pthread_attr_t));
    cpu_set_t cpus;
    pthread_attr_init(attr);
    CPU_ZERO(&cpus);
    CPU_SET(i, &cpus); // CPU 0
    pthread_attr_setaffinity_np(attr, sizeof(cpu_set_t), &cpus);

    ret = pthread_create(&tid[i], attr, iothread, &args[i]);
    if (ret != 0) { 
      fprintf(stderr, "thread exit\n");
      free(attr);
      return FAILED;
    }
    pthread_attr_destroy(attr);
    free(attr);
  }

  for(int i = 0; i < t; i++) {
    pthread_join(tid[i], 0);
  }

  if(op_type == READ_OP) {
    clock_gettime(CLOCK_REALTIME, &t2);
    unsigned long long start, end;
    start = t1.tv_sec * 1000000000L + t1.tv_nsec;
    end = t2.tv_sec * 1000000000L + t2.tv_nsec;
    double sec = (double)(end - start) / 1000000000L;
    fprintf(stdout, "Total time %.2f sec; Throughput %.2f ops/sec\n", sec, (double) num_rds * t /sec );
  }
  if(op_type == WRITE_OP) {
    clock_gettime(CLOCK_REALTIME, &t2);
    unsigned long long start, end;
    start = t1.tv_sec * 1000000000L + t1.tv_nsec;
    end = t2.tv_sec * 1000000000L + t2.tv_nsec;
    double sec = (double)(end - start) / 1000000000L;
    fprintf(stdout, "Total time %.2f sec; Throughput %.2f ops/sec\n", sec, (double) num_ios * t /sec );
  }

  stats_end.store(true) ;
  sleep(1);
  stat_thread.join();
  fprintf(stdout, "Total operation keys %lu\n", ops_keys.load());

#ifdef PERF_RD_LAT
  proc_rd_lat(t, args);
#endif

  delete db;

  free(keys);
  
  return SUCCESS;
}