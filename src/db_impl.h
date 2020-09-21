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
#include "blockingconcurrentqueue.h"

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
  void notifyAll() {
    std::unique_lock<std::mutex> lck(mtx_);
    ready_ = true;
    cv_.notify_all();
  }
  void wait() {
    std::unique_lock<std::mutex> lck(mtx_);
    while (!ready_) cv_.wait(lck);
  }
};

struct packKVEntry {
  uint64_t seq;
  int size;
  std::string key;
  std::string value;
  packKVEntry(int _size, const Slice& _key, const Slice& _val)
              :size(_size) {
                key = _key.ToString();
                value = _val.ToString();
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
  const Options options_;

  kvssd::KVSSD *kvd_;
  KVIndex *key_idx_[MAX_INDEX_NUM];

  // monotonous seqence
  uint64_t sequence_; // packed KV physical key
  std::mutex seq_mutex_;
  moodycamel::BlockingConcurrentQueue<packKVEntry*> pack_q_;
  Monitor pack_q_wait_; // maintain queue depth
  // consumer threads
  int pack_threads_num;
  std::thread **pack_threads_;
  std::mutex *thread_m_;
  bool *shutdown_;

  void processQ(int id);
  void save_meta() {
    std::string meta;
    meta.append((char*)&sequence_, sizeof(uint64_t));
    kvssd::Slice meta_key("KVRangeDB_meta");
    kvssd::Slice meta_val;
    kvd_->kv_store(&meta_key, &meta_val);
    printf("Finish saving KVRangeDB meta\n");
  }
  bool load_meta(uint64_t &seq) { 
    char *vbuf;
    int vsize;
    kvssd::Slice meta_key("KVRangeDB_meta");
    int found = kvd_->kv_get(&meta_key, vbuf, vsize); 
    if (found != 0) {
      free(vbuf);
      printf("New KVRangeDB created\n");
      return false; // no meta;
    }
    else {
      free(vbuf);
      seq = *((uint64_t*)vbuf);
      printf("Load KVRangeDB meta\n");
      return true;
    }

  }

  bool do_check_filter(const Slice& key) {
    /* NOT IMPLEMENT */
    return false;
  }
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
