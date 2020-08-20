/******* kvrangedb *******/
/* db_impl.h
* 07/23/2019
* by Mian Qin
*/

#ifndef _db_impl_h_
#define _db_impl_h_

#include <stdint.h>
#include <stdio.h>
#include <string>
#include "kvrangedb/db.h"
#include "kvssd/kvssd.h"
#include "kv_index.h"

#define MAX_INDEX_NUM 8

namespace kvrangedb {

// Monitor for async I/O
class Monitor {
public:
  std::mutex mtx_;
  std::condition_variable cv_;
  bool ready_ ;
  Monitor() : ready_(false) {}
  ~Monitor(){}
  void reset() {ready_ = false;};
  void notify() {
    std::unique_lock<std::mutex> lck(mtx_);
    ready_ = true;
    cv_.notify_one();
  }
  void wait() {
    std::unique_lock<std::mutex> lck(mtx_);
    while (!ready_) cv_.wait(lck);
  }
};

class DBImpl : public DB{
friend class DBIterator;
public:
  DBImpl(const Options& options, const std::string& dbname);
  ~DBImpl();

  // Implementations of the DB interface
  Status Put(const WriteOptions&, const Slice& key, const Slice& value);
  Status Delete(const WriteOptions&, const Slice& key);
  Status Write(const WriteOptions& options, WriteBatch* updates);
  Status Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value);
  Iterator* NewIterator(const ReadOptions&);

  kvssd::KVSSD* GetKVSSD() {return kvd_;}
  KVIndex* GetKVIndex(int id) {return key_idx_[id];}
  const Comparator* GetComparator() {return options_.comparator;}

private:
  kvssd::KVSSD *kvd_;
  KVIndex *key_idx_[MAX_INDEX_NUM];

  const Options options_;

public:
  // DEBUG ONLY
  void close_idx () {
    for (int i = 0; i < options_.indexNum; i++)
    delete key_idx_[i];
  }
  void open_idx() {
    for (int i = 0; i < options_.indexNum; i++) {
      std::string indexName = std::to_string(i);
      if (options_.indexType == LSM)
        key_idx_[i] = NewLSMIndex(options_, kvd_, indexName);
      else if (options_.indexType == LSMOPT)
        key_idx_[i] = NewLSMIndex(options_, kvd_, indexName);
      else if (options_.indexType == BTREE)
        key_idx_[i] = NewBTreeIndex(options_, kvd_, indexName);
      else if (options_.indexType == BASE) {
        key_idx_[i] = NewBaseIndex(options_, kvd_, indexName);
      }
      else if (options_.indexType == INMEM) {
        key_idx_[i] = NewInMemIndex(options_, kvd_, indexName);
      }
      else {
        printf("WRONG KV INDEX TYPE\n");
        exit(-1);
      }
    }
  }
};


}  // namespace kvrangedb



#endif