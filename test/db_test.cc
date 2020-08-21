
/* db_test.cc
* 07/29/2019
* by Mian Qin
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <new>
#include <assert.h>
#include <unistd.h>
#include <thread>
#include <map>
#include <ctime>
#include <chrono>
#include <iostream>

#include "kvrangedb/slice.h"
#include "kvrangedb/comparator.h"
#include "kvrangedb/db.h"

#define OBJ_LEN 1024

std::map<std::string, std::string> groundTruth;

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

void DoWrite(kvrangedb::DB *db, int num, bool seq, bool batchIDX) {
    RandomGenerator gen;
    Random rand(0);
    kvrangedb::WriteOptions wropts;
    wropts.batchIDXWrite = batchIDX;
    for (int i = 0; i < num; i++) {
        const int k = seq ? i : (rand.Next() % num);
        char key[100];
        snprintf(key, sizeof(key), "%016d", k);
        char *value = gen.Generate(OBJ_LEN);
        kvrangedb::Slice val(value, OBJ_LEN);

        db->Put(wropts, key, val);
        //printf("[insert] key %s, val %s\n", key, std::string(value, 8).c_str());
        groundTruth[std::string(key, 16)] = std::string(value, 8).c_str();
    }
}

void DoDelete(kvrangedb::DB *db, int num, bool seq) {
    RandomGenerator gen;
    Random rand(0);
    kvrangedb::WriteOptions wropts;
    for (int i = 0; i < num; i++) {
        const int k = seq ? i : (rand.Next() % num);
        char key[100];
        snprintf(key, sizeof(key), "%016d", k);

        db->Delete(wropts, key);
        //printf("[delete] key %s\n", key);
    }
}

void RandomRead(kvrangedb::DB *db, int num) {
  Random rand(0);
  kvrangedb::ReadOptions rdopts;
  for (int i = 0; i < 100; i++) {
      char key[100];
      const int k = (rand.Next() % num);
      snprintf(key, sizeof(key), "%016d", k);

      std::string val;
      db->Get(rdopts, key, &val);
      printf("[get] key %s, val %s, val_len %d\n", key, val.substr(0,8).c_str(), val.size());
 
      printf("[truth] key %s, val %s, val_len %d\n", key, groundTruth[std::string(key, 16)].c_str(), val.size());
  }
}

void RandomSeek(kvrangedb::DB *db, int num, int seed) {
  kvrangedb::ReadOptions rdopts;
  int found = 0;
  int seek_num = 10;
  Random rand(seed);
  for (int i = 0; i < seek_num; i++) {
    kvrangedb::Iterator* iter = db->NewIterator(rdopts);
    char key[100];
    const int k = (rand.Next() % num);
    snprintf(key, sizeof(key), "%016d", k);
    
    iter->Seek(key);
    if (iter->Valid() && iter->key() == key) found++;
    kvrangedb::Slice val = iter->value();
    printf("Seek %d, Get key %s, value %s\n", k, iter->key().ToString().c_str(), std::string(val.data(), 8).c_str());

    delete iter;
    
    auto itlow=groundTruth.lower_bound (std::string(key, 16));
    printf("Seek %d, Truth key %s, value %s\n", k, itlow->first.c_str(), itlow->second.c_str());
  }
  printf("%d out of %d keys found\n", found, seek_num);

}

void DoScan(kvrangedb::DB *db, int num) {
  kvrangedb::ReadOptions rdopts;
  Random rand(2020);
  kvrangedb::Iterator* iter = db->NewIterator(rdopts);
  int scan_len = 10;
  
  char key[100] = {0};
  const int k = (rand.Next() % num);
  snprintf(key, sizeof(key), "%016d", k);
  //iter->SeekToFirst();
  iter->Seek(key);
  int i = 0;
  printf("Scan start key %s\n", key);
  auto itlow=groundTruth.lower_bound (std::string(key, 16));
  while(iter->Valid() && scan_len > 0) {
    kvrangedb::Slice val = iter->value();
    printf("[Scan] #%d, Get key %s, value %s\n", ++i, iter->key().ToString().c_str(), std::string(val.data(), 8).c_str());
    printf("[Scan] #%d, Truth key %s, value %s\n", i, itlow->first.c_str(), itlow->second.c_str());

    iter->Next();
    ++itlow;
    scan_len--;
  }
  delete iter;
}

class CustomComparator : public kvrangedb::Comparator {
public:
  CustomComparator() {}
  ~CustomComparator() {}
  int Compare(const kvrangedb::Slice& a, const kvrangedb::Slice& b) const {
    return a.compare(b);
  }
};

int main () {
  int num = 1330000;
  int thread_cnt = 1;

  CustomComparator cmp;
  kvrangedb::Options options;
  options.comparator = &cmp;
  options.cleanIndex = true;
  options.indexNum = 1;

  kvrangedb::DB *db = NULL;
  //kvrangedb::DB::Open(options, "/dev/kvemul", &db);
  kvrangedb::DB::Open(options, "/dev/nvme1n1", &db);

  auto wcts = std::chrono::system_clock::now();
  DoWrite(db, num, false, true);
  std::chrono::duration<double> wctduration = (std::chrono::system_clock::now() - wcts);

  sleep(1); // wait for write done
  // db->close_idx(); // close db index
  // // open db index again
  // db->open_idx();

  RandomRead(db, num);

  std::thread *th_seek[16];
  RandomSeek(db, num, 2019);
  for (int i = 0; i< thread_cnt; i++) {
    th_seek[i] = new std::thread(RandomSeek, db, num, i);
  }
  for (int i = 0; i< thread_cnt; i++) {
    th_seek[i]->join();
  }

  DoScan(db, num);
  for (int i = 0; i< thread_cnt; i++) {
    delete th_seek[i];
  }
  
  std::cout << "DoWrite finished in " << wctduration.count() << " seconds [Wall Clock]" << std::endl;
  delete db;
  return 0;
}
