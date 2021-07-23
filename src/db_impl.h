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
#include <unordered_map>
#include "kvrangedb/db.h"
#include "kvssd/kvssd.h"
#include "kv_index.h"
#include "blockingconcurrentqueue.h"
#include "filter.h"
#include "cache/cache.h"

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
  int size;
  std::string key;
  std::string value;
  Monitor *mon;
  packKVEntry(int _size, const Slice& _key, const Slice& _val, Monitor *_mon)
              :size(_size), mon(_mon) {
                key = _key.ToString();
                value = _val.ToString();
              }
};

struct syncPackKVEntry {
  uint64_t phyKey;
  std::string key;
  std::string value;
  syncPackKVEntry(uint64_t _seq, const Slice& _key, const Slice& _val)
              :phyKey(_seq) {
                key = _key.ToString();
                value = _val.ToString();
              }
};

class CacheEntry { 
public:
    char *val;
    int size;
    CacheEntry() : val(NULL), size(0){};
    CacheEntry(char *v, int s) : size(s) {
        val = (char *)malloc(s);
        memcpy(val, v, size);
    }
    ~CacheEntry() {if(val) free(val);}
};
template <class T>
static void DeleteEntry(const Slice& /*key*/, void* value) {
  T* typed_value = reinterpret_cast<T*>(value);
  delete typed_value;
}

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
  Options options_;

  kvssd::KVSSD *kvd_;
  KVIndex *key_idx_[MAX_INDEX_NUM];

  // monotonous seqence
  uint64_t sequence_; // packed KV physical key
  std::mutex seq_mutex_;
  moodycamel::BlockingConcurrentQueue<packKVEntry*> pack_q_;
  Monitor pack_q_wait_; // maintain queue depth

  // sync packing queue (same packID always in the same thread)
  std::mutex sq_mutex_;
  std::unordered_map<int64_t, std::vector<syncPackKVEntry>> sq_;

  // consumer threads
  int pack_threads_num;
  std::thread **pack_threads_;
  std::mutex *thread_m_;
  bool *shutdown_;
  
  std::mutex hk_mutex_;
  std::unordered_map<std::string, int> hot_keys_;
  std::atomic<int> hot_keys_training_cnt_;
  std::string bf_;

  // in-memory cache
  Cache *cache_;

  // Range filter
  RangeFilter *rf_;

  // I/O request conter (read only for now)
  std::atomic<int64_t> inflight_io_count_;

  // Total records counter
  std::atomic<int64_t> total_record_count_;
  std::atomic<int64_t> packed_record_count_;
  std::atomic<bool> bg_compact_shutdown_;
  std::thread *bg_compact_thread_;

  uint64_t get_new_seq() {
    uint64_t seq; 
    {
        std::unique_lock<std::mutex> lock(seq_mutex_);
        seq = sequence_++;
    }   
    return seq;
  }

  uint64_t get_curr_seq() {
    return sequence_;
  }


  void processQ(int id);
  void save_meta() {
    std::string meta;
    meta.append((char*)&sequence_, sizeof(uint64_t));
    kvssd::Slice meta_key("KVRangeDB_meta");
    kvssd::Slice meta_val(meta);
    kvd_->kv_store(&meta_key, &meta_val);
    printf("Finish saving KVRangeDB meta\n");

    if (bf_.size() && options_.manualCompaction) {
      const int MAX_V_SIZE = 2<<20; // 2MB max value size
      char *p = &bf_[0];
      int write_bytes = 0;
      int bf_kv_cnt = 0;
      while(write_bytes < bf_.size()) {
        int vlen = bf_.size() - write_bytes > MAX_V_SIZE ? MAX_V_SIZE : bf_.size() - write_bytes;
        std::string bf_key_str = "KVRangeDB_bf"+std::to_string(bf_kv_cnt++);
        kvssd::Slice bf_key(bf_key_str);
        kvssd::Slice bf_val(p, vlen);
        kvd_->kv_store(&bf_key, &bf_val);
        p += vlen;
        write_bytes += vlen;
      }
      printf("Finish saving Bloom Filter, total size %d\n", bf_.size());
    }
  }
  bool load_meta(uint64_t &seq) { 
    bool newDB;
    char *vbuf;
    int vsize;
    kvssd::Slice meta_key("KVRangeDB_meta");
    int found = kvd_->kv_get(&meta_key, vbuf, vsize); 
    if (found != 0) {
      free(vbuf);
      printf("New KVRangeDB created\n");
      newDB = false; // no meta;
    }
    else {
      seq = *((uint64_t*)vbuf);
      free(vbuf);
      printf("Load KVRangeDB meta, seq# %llu\n", seq);
      newDB = true;
    }

    // load bloom filter
    int bf_kv_cnt = 0;
    while (true) {
      std::string bf_key_str = "KVRangeDB_bf"+std::to_string(bf_kv_cnt++);
      kvssd::Slice bf_key(bf_key_str);
      int found = kvd_->kv_get(&bf_key, vbuf, vsize); 
      if (found == 0) {
        bf_.append(vbuf, vsize);
        free(vbuf);
      }
      else {
        printf("Finish loading bloom filter, total size %d\n", bf_.size());
        break;
      }
    }
    return newDB;
  }

  bool do_check_filter(const Slice& key) {
    
    if (bf_.size()) {
      BloomFilter bf(options_.filterBitsPerKey);
      return bf.KeyMayMatch(key, bf_);
    }
    else { // mo filter
      return false;
    }
    
  }

  void ManualCompaction();
  void BGCompaction();
  void DoBGCompact(std::vector<std::string>* klist, int offset, int size);
  void BuildBloomFilter();
  void BuildRangeFilter();

  // in-memory cache interface
	Cache::Handle* read_cache(std::string& key, std::string* value) {
        if (cache_==NULL) return NULL;
        Cache::Handle *h = cache_->Lookup(key);
        if (h != NULL) {
            CacheEntry *rd_val = reinterpret_cast<CacheEntry*>(cache_->Value(h));
            value->append(rd_val->val, rd_val->size);
            RecordTick(options_.statistics.get(), CACHE_HIT);
        }
        else 
            RecordTick(options_.statistics.get(), CACHE_MISS);
        return h;
    };
    Cache::Handle* insert_cache(std::string& key, const Slice& value) {
        if (cache_==NULL) return NULL;
        CacheEntry *ins_val = new CacheEntry((char *)value.data(), value.size());
        size_t charge = sizeof(CacheEntry) + value.size();
        Cache::Handle *h = cache_->Insert(key, reinterpret_cast<void*>(ins_val), charge, DeleteEntry<CacheEntry>);

        RecordTick(options_.statistics.get(), CACHE_FILL);
        return h;
    };
    void erase_cache(std::string& key) {
        if (cache_==NULL) return ;
        bool evicted = cache_->Erase(key);

        if (evicted) RecordTick(options_.statistics.get(), CACHE_ERASE);
    };
    void release_cache(Cache::Handle* h) {
        if (cache_==NULL) return ;
        cache_->Release(h);
    };

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
