/******* kvrangedb *******/
/* db_impl.cc
* 07/23/2019
* by Mian Qin
*/
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <chrono>
#include "kvrangedb/db.h"
#include "kvrangedb/iterator.h"
#include "kvrangedb/write_batch.h"
#include "db_impl.h"
#include "db_iter.h"
#include "hash.h"

#include <endian.h>

// meant to measure the benefit of Hot query acceleration
// extern int hitCnt;
// extern double hitCost;
// extern double missCost;
// extern double hitNextCost;
// extern double missNextCost;
namespace kvrangedb {

// WriteBatch definition
WriteBatch::WriteBatch() {}
WriteBatch::~WriteBatch() {}

void WriteBatch::Put(const Slice& key, const Slice& value) {
    batch_.push_back(std::make_pair(key.ToString(), value.ToString()));
}
void WriteBatch::Delete(const Slice& key) {
    batch_.push_back(std::make_pair(key.ToString(), std::string()));
}
void WriteBatch::Clear() {
    batch_.clear();
}
int WriteBatch::Size() {
    return batch_.size();
}

static void on_io_complete(void *args) {
    Monitor *mon = (Monitor *)args;
    mon->notify();
}

DBImpl::DBImpl(const Options& options, const std::string& dbname) 
: options_(options),
  sequence_(0),
  pack_threads_num(options.packThreadsNum),
  hot_keys_training_cnt_(0),
  inflight_io_count_(0),
  total_record_count_(0),
  packed_record_count_(0) {
  kvd_ = new kvssd::KVSSD(dbname.c_str(), options_.statistics.get());
  for (int i = 0; i < options.indexNum; i++) {
    std::string indexName = std::to_string(i);
    if (options.indexType == LSM) {
      key_idx_[i] = NewLSMIndex(options, kvd_, indexName);
    }
    else if (options.indexType == LSMOPT) {
      key_idx_[i] = NewLSMIndex(options, kvd_, indexName);
    }
    else if (options.indexType == ROCKS) {
      key_idx_[i] = NewRocksIndex(options, kvd_, indexName);
    }
    else if (options.indexType == BTREE) {
      key_idx_[i] = NewBTreeIndex(options, kvd_, indexName);
    }
    else if (options.indexType == BASE) {
      key_idx_[i] = NewBaseIndex(options, kvd_, indexName);
    }
    else if (options.indexType == INMEM) {
      key_idx_[i] = NewInMemIndex(options, kvd_, indexName);
    }
    else {
      printf("WRONG KV INDEX TYPE\n");
      exit(-1);
    }
  } 
  // setup stats dump
  options_.statistics.get()->setStatsDump(options_.stats_dump_interval);
  // load meta (seq and bloom filter)
  bool newDB = load_meta(sequence_);

  if (!newDB && bf_.size() == 0 && options_.bfHotKeyNum > 0) {
    CreateEmtpyBloomFilter(options_.bfHotKeyNum);
  }

  if (!options_.packThreadsDisable) {
    pack_threads_ = new std::thread*[pack_threads_num];
    thread_m_ = new std::mutex[pack_threads_num];
    shutdown_ = new bool[pack_threads_num];
    for (int i = 0; i < pack_threads_num; i++) {
      shutdown_[i] = false;
      pack_threads_[i] = new std::thread(&DBImpl::processQ, this, i);
      printf("Initiated worker thread %d\n", i);
    }
    printf("Max Pack Size: %d\n", options_.packSize);
  } else {
    printf("Background packing threads disabled\n");
  }

  // initialize BG compaction thread
  bg_compact_shutdown_.store(false, std::memory_order_relaxed);
  
  if (options_.bgCompaction) {
    bg_compact_thread_ = new std::thread(&DBImpl::BGCompaction, this);
    printf("Starting background compaction thread\n");
  }
  else {
    printf("Background compaction thread disabled\n");
  }

  // allocate write buffer (2 times size of packing queue)
  write_buffer_ = NewFIFOCache((size_t)options_.packQueueDepth * 2 + 16, 4); // cap unit: entry
  // initialize in-memory data cache
  if (options.dataCacheSize > 0) {
    cache_ = NewLRUCache((size_t)options.dataCacheSize << 20, -1);
  }
  else {
    cache_ = nullptr;
  }

  // initialize range filter
  if (options.rfType == HiBloom) {
    rf_ = NewHiBloomFilter(options.rfBitsPerKey, options.rfBitsPerLevel, options.rfLevels,
    options.rfExamBits, options.rfNumKeys, options.rfMaxProbes, options_.statistics.get());
  }
  else if (options.rfType == RBloom) {
    rf_ = NewRBloomFilter(options.rfBitsPerKey, options.rfExamBits, options.rfNumKeys, 
    options_.statistics.get());
  }
  else { // NoFilter
    rf_ = nullptr;
  }
  if (rf_) BuildRangeFilter();
}

DBImpl::~DBImpl() {
  RecordTick(options_.statistics.get(), DEV_UTIL, kvd_->get_dev_util());
  if (options_.cleanIndex) {
    for (int i = 0; i < options_.indexNum; i++) {
      std::string meta_name = std::to_string(i)+"/CURRENT";
      kvssd::Slice del_key_lsm(meta_name);
      kvd_->kv_delete(&del_key_lsm);
      printf("Clean index\n");
    }
  }
  
  if (!(options_.packThreadsDisable)) sleep(1);

  // save range filter temporarily if needed
  if (rf_) {
    std::string filter_name = rf_->GenFilterName();
    std::ifstream f(filter_name.c_str());
    if(!f.good()) {
      rf_->SaveFilter(filter_name);
      printf("Save range filter as %s\n", filter_name.c_str());
    }
    else {
      printf("Range filter as %s already saved\n", filter_name.c_str());
    }
  }


  if (options_.manualCompaction) {
    printf("Start ManualCompaction\n");
    ManualCompaction();
    printf("Start filter building\n");
    BuildBloomFilter();
  }

  // flush sync queue
  flush_sync_queue();

  // shutdown packing threads
  if (!options_.packThreadsDisable) {
    for (int i = 0; i < pack_threads_num; i++) { 
        {
            std::unique_lock<std::mutex> lck (thread_m_[i]);
            shutdown_[i] = true;
        }
    }

    for (int i = 0; i < pack_threads_num; i++) { 
        pack_threads_[i]->join();
        delete pack_threads_[i];
        printf("Shutdown worker thread %d\n", i);
    }

    delete [] pack_threads_;
    delete [] thread_m_;
    delete [] shutdown_;
  }

  // shutdown BG compaction thread
  if (options_.bgCompaction) {
    bg_compact_shutdown_.store(true, std::memory_order_relaxed);
    bg_compact_thread_->join();
  }

  // save meta (sequence number)
  save_meta();
  delete write_buffer_;
  delete cache_;

  for (int i = 0; i < options_.indexNum; i++)
    delete key_idx_[i];
	delete kvd_;
}

// bulk dequeue, either dequeue max_size or wait for time out
template <class T> 
static int dequeue_bulk_timed(moodycamel::BlockingConcurrentQueue<T*> &q, 
    std::vector<T*>& kvs, size_t max,
    size_t max_size, int64_t timeout_usecs) {
    int total_size = 0;
    const uint64_t quanta = 100;
    const double timeout = ((double)timeout_usecs - quanta) / 1000000;
    auto start = std::chrono::system_clock::now();
    auto elapsed = [start]() -> double {
        return std::chrono::duration<double>(std::chrono::system_clock::now() - start).count();
    };
    do
    {
      T *item;
      bool found = q.wait_dequeue_timed(item, quanta);
      if (found) {
        total_size += item->size;
        kvs.push_back(item);
      }
    } while (total_size < max_size && kvs.size() < max && elapsed() < timeout);

    return total_size;
};

static void do_pack_KVs (uint64_t seq, std::vector<packKVEntry*>& kvs, int pack_size,
        kvssd::Slice& pack_key, kvssd::Slice& pack_val, IDXWriteBatch *index_batch) {
    
    char* pack_key_str = (char*) malloc(sizeof(uint64_t));
    char* pack_val_str = (char*) malloc(pack_size);
    // key
    *((uint64_t*)pack_key_str) = seq;
    pack_key = kvssd::Slice (pack_key_str, sizeof(uint64_t));
    Slice pkey(pack_key_str, sizeof(uint64_t));

    // value
    char *p = pack_val_str;
    for (int i = 0; i < kvs.size(); i++) {
      Slice lkey(kvs[i]->key);
      index_batch->Put(lkey, pkey);

      *((uint8_t*)p) = kvs[i]->key.size();
      p += sizeof(uint8_t);
      memcpy(p, kvs[i]->key.data(), kvs[i]->key.size());
      p += kvs[i]->key.size();
      *((uint32_t*)p) = kvs[i]->value.size();
      p += sizeof(uint32_t);
      memcpy(p, kvs[i]->value.data(), kvs[i]->value.size());
      p += kvs[i]->value.size();

    }
    assert((int)(p -pack_val_str) == pack_size);
    pack_val = kvssd::Slice (pack_val_str, pack_size);

    return;
};

static void do_pack_KVs (std::vector<syncPackKVEntry>* kvs,
        kvssd::Slice& pack_key, kvssd::Slice& pack_val, IDXWriteBatch *index_batch) {
    int pack_size = 0;
    for (int i = 0; i < kvs->size(); i++) {
      pack_size += ((*kvs)[i].key.size() + (*kvs)[i].value.size());
    }

    char* pack_key_str = (char*) malloc(sizeof(uint64_t));
    char* pack_val_str = (char*) malloc(pack_size);
    // key
    *((uint64_t*)pack_key_str) = (*kvs)[0].phyKey;
    pack_key = kvssd::Slice (pack_key_str, sizeof(uint64_t));
    Slice pkey(pack_key_str, sizeof(uint64_t));

    // value
    char *p = pack_val_str;
    for (int i = 0; i < kvs->size(); i++) {
      Slice lkey((*kvs)[i].key);
      index_batch->Put(lkey, pkey);

      *((uint8_t*)p) = (*kvs)[i].key.size();
      p += sizeof(uint8_t);
      memcpy(p, (*kvs)[i].key.data(), (*kvs)[i].key.size());
      p += (*kvs)[i].key.size();
      *((uint32_t*)p) = (*kvs)[i].value.size();
      p += sizeof(uint32_t);
      memcpy(p, (*kvs)[i].value.data(), (*kvs)[i].value.size());
      p += (*kvs)[i].value.size();

    }
    assert((int)(p -pack_val_str) == pack_size);
    pack_val = kvssd::Slice (pack_val_str, pack_size);

    return;
};

static bool do_unpack_KVs (char *vbuf, int size, const Slice& lkey, std::string* lvalue) {
  char *p = vbuf;
  while ((p-vbuf) < size) {
    uint8_t key_len = *((uint8_t*)p);
    p += sizeof(uint8_t);
    Slice extract_key (p, key_len);
    p += key_len;
    uint32_t val_len = *((uint32_t*)p);
    p += sizeof(uint32_t);
    if (lkey.compare(extract_key) == 0) {
      lvalue->append(p, val_len);
      return true;
    } else {
      p += val_len;
    }
    assert ((p-vbuf) <= size);
  }
  return false;
}

void DBImpl::processQ(int id) {
  bool shutdown = false;
  bool ready_to_shutdown = false;
  int sleep_cnt = 0;
  while (true && (!shutdown)) {
    // check thread shutdown
    {
      std::unique_lock<std::mutex> lck (thread_m_[id]);
      if (shutdown_[id] == true && (!ready_to_shutdown)) {
          ready_to_shutdown = true;
      }
    }

    if (ready_to_shutdown) {
      sleep_cnt++;
      sleep(1); // wait all enqueue done
    }
    
    // dequeue
    std::vector<packKVEntry*> kvs;
    int pack_size = dequeue_bulk_timed(pack_q_, kvs, options_.maxPackNum, options_.packSize, options_.packDequeueTimeout);
    if (kvs.size()) {
      uint64_t seq;
      seq = get_new_seq();
      
      // pack value
      kvssd::Slice pack_key;
      kvssd::Slice pack_val;
      IDXWriteBatch *index_batch ;
      if (options_.indexType == LSM) {
        index_batch = NewIDXWriteBatchLSM();
      }
      else if (options_.indexType == LSMOPT) {
        index_batch = NewIDXWriteBatchLSM();
      }
      else if (options_.indexType == ROCKS) {
        index_batch = NewIDXWriteBatchRocks();
      }
      else if (options_.indexType == BTREE) {
        index_batch = NewIDXWriteBatchBTree();
      }
      else if (options_.indexType == BASE) {
        index_batch = NewIDXWriteBatchBase();
      }
      else if (options_.indexType == INMEM) {
        index_batch = NewIDXWriteBatchInmem();
      }

      do_pack_KVs(seq, kvs, pack_size, pack_key, pack_val, index_batch);
      
      // phyKV write
      // Monitor mon;
      // kvd_->kv_store_async(&pack_key, &pack_val, on_io_complete, &mon);
      
      // // index write
      // key_idx_[0]->Write(index_batch); // pack only support single index tree

      // mon.wait(); // wait data I/O done

      // sync write 1-data, 2-index
      kvd_->kv_store(&pack_key, &pack_val);
      key_idx_[0]->Write(index_batch); // pack only support single index tree
      
      // clean up
      for (int i = 0; i < kvs.size(); i++) {
        if (kvs[i]->mon) kvs[i]->mon->notify();
        delete kvs[i]; // de-allocate KV buffer;
      }
      free((char*) pack_key.data());
      free((char*) pack_val.data());
      delete index_batch;

      if (pack_q_.size_approx() < options_.packQueueDepth) 
      pack_q_wait_.notifyAll();
    }
    else { // check shutdown
        if (ready_to_shutdown && (pack_q_.size_approx() == 0||sleep_cnt>=16)) { // in case size_approx not zero
            shutdown = true;
            break;
        }
    }
  }
}

inline int get_phyKV_size(const Slice& key, const Slice& value) {
  return key.size()+value.size()+sizeof(uint8_t)+sizeof(uint32_t);
}

void DBImpl::flush_sync_queue() {
  // Call when closing DB
  // in single thread context
  for (int i = 0; i < SYNC_Q_SHARD; i++) {
    
    for (auto it = sq_[i].begin(); it != sq_[i].end(); ++it) {
      if (it->second.size() == 1) { // no packing
        kvssd::Slice put_key(it->second[0].key.data(), it->second[0].key.size());
        kvssd::Slice put_val(it->second[0].value.data(), it->second[0].value.size());

        if (options_.bfHotKeyNum > 0) { // dynamic build hotkey filter
          Slice key(it->second[0].key);
          InsertEntryBloomFilter(key);
        }

        // sync write 1-data, 2-index
        kvd_->kv_store(&put_key, &put_val);
        int idx_id = (options_.indexNum == 1) ? 0 : MurmurHash64A(it->second[0].key.data(), it->second[0].key.size(), 0)%options_.indexNum;
        key_idx_[idx_id]->Put(it->second[0].key);
      } else { // pack the rest
        kvssd::Slice pack_key;
        kvssd::Slice pack_val;
        IDXWriteBatch *index_batch ;
        std::vector<syncPackKVEntry> *packGroup = &(it->second);
        if (options_.indexType == LSM) {
          index_batch = NewIDXWriteBatchLSM();
        }
        else if (options_.indexType == LSMOPT) {
          index_batch = NewIDXWriteBatchLSM();
        }
        else if (options_.indexType == ROCKS) {
          index_batch = NewIDXWriteBatchRocks();
        }
        else if (options_.indexType == BTREE) {
          index_batch = NewIDXWriteBatchBTree();
        }
        else if (options_.indexType == BASE) {
          index_batch = NewIDXWriteBatchBase();
        }
        else if (options_.indexType == INMEM) {
          index_batch = NewIDXWriteBatchInmem();
        }

        do_pack_KVs(packGroup, pack_key, pack_val, index_batch);
        RecordTick(options_.statistics.get(), PACK_CNT);
        RecordTick(options_.statistics.get(), PACK_RECORD_CNT, packGroup->size());
        RecordTick(options_.statistics.get(), PACK_BYTES, pack_val.size());

        // sync write 1-data, 2-index
        kvd_->kv_store(&pack_key, &pack_val);
        key_idx_[0]->Write(index_batch); // pack only support single index tree

        // clean up pack buffer
        free((void*)pack_key.data());
        free((void*)pack_val.data());
        
      }
      // no need to erase sync queue entry since we are closing
    }
  }

}

uint64_t hash64(uint64_t x) {
    x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
    x = x ^ (x >> 31);
    return x;
}

Status DBImpl::Put(const WriteOptions& options,
                     const Slice& key,
                     const Slice& value) {
  RecordTick(options_.statistics.get(), REQ_PUT);
  total_record_count_.fetch_add(1, std::memory_order_relaxed);
  //printf("KVRangeDB Put %s\n", std::string(key.data(), key.size()).c_str());

  std::string skey(key.data(), key.size());

  if (options.update && (value.size() >= options_.packThres || options_.packThreadsDisable)) {
    erase_cache_entry(skey);
    Cache::Handle* h = insert_cache(skey, value);
    release_cache_entry(h);
    
    // update to write buffer
    erase_buffer_entry(skey);
    Cache::Handle* bh = insert_buffer(skey, value);
    release_buffer_entry(bh);

    kvssd::Slice put_key(key.data(), key.size());
    kvssd::Slice put_val(value.data(), value.size());

    // only update data not index
    kvd_->kv_store(&put_key, &put_val);
  }
  else {
    // insert to in-memory cache
    Cache::Handle* h = insert_cache(skey, value);
    release_cache_entry(h);

    // insert to write buffer
    Cache::Handle* bh = insert_buffer(skey, value);
    release_buffer_entry(bh);

    // sync packing
    if (options.packID >=0) {
      uint64_t seq;
      //int sq_shard_id = hash64(options.packID)%SYNC_Q_SHARD;
      int sq_shard_id = options.packID%SYNC_Q_SHARD;
      {
        std::unique_lock<std::mutex> lock(sq_mutex_[sq_shard_id]);
        int sq_entries = sq_[sq_shard_id].size();

        // heavy lifting flush, require blocking
        if (sq_entries >= SYNC_Q_FLUSH_THRES) { // 1, packID mono increase; 2, same thread context on same packID; 3, flush the smallest entry (group)
          auto head = sq_[sq_shard_id].begin();
          if (head->second.size() == 1) { // no packing
            kvssd::Slice put_key(head->second[0].key.data(), head->second[0].key.size());
            kvssd::Slice put_val(head->second[0].value.data(), head->second[0].value.size());

            if (options_.bfHotKeyNum > 0) { // dynamic build hotkey filter
              Slice key(head->second[0].key);
              InsertEntryBloomFilter(key);
            }

            // sync write 1-data, 2-index
            kvd_->kv_store(&put_key, &put_val);
            int idx_id = (options_.indexNum == 1) ? 0 : MurmurHash64A(head->second[0].key.data(), head->second[0].key.size(), 0)%options_.indexNum;
            key_idx_[idx_id]->Put(head->second[0].key);
          } else {
            kvssd::Slice pack_key;
            kvssd::Slice pack_val;
            IDXWriteBatch *index_batch ;
            if (options_.indexType == LSM) {
              index_batch = NewIDXWriteBatchLSM();
            }
            else if (options_.indexType == LSMOPT) {
              index_batch = NewIDXWriteBatchLSM();
            }
            else if (options_.indexType == ROCKS) {
              index_batch = NewIDXWriteBatchRocks();
            }
            else if (options_.indexType == BTREE) {
              index_batch = NewIDXWriteBatchBTree();
            }
            else if (options_.indexType == BASE) {
              index_batch = NewIDXWriteBatchBase();
            }
            else if (options_.indexType == INMEM) {
              index_batch = NewIDXWriteBatchInmem();
            }

            do_pack_KVs(&(head->second), pack_key, pack_val, index_batch);
            RecordTick(options_.statistics.get(), PACK_CNT);
            RecordTick(options_.statistics.get(), PACK_RECORD_CNT, head->second.size());
            RecordTick(options_.statistics.get(), PACK_BYTES, pack_val.size());


            // sync write 1-data, 2-index
            kvd_->kv_store(&pack_key, &pack_val);
            key_idx_[0]->Write(index_batch); // pack only support single index tree

            // clean up pack buffer
            free((void*)pack_key.data());
            free((void*)pack_val.data());

            
          }
          // erase group in the sync queue
          sq_[sq_shard_id].erase (head);  
        }
   
        std::vector<syncPackKVEntry> *packGroup;
        // work on the current entry
        packGroup = &(sq_[sq_shard_id][options.packID]);
        if (packGroup->size() == 0) {
          seq = get_new_seq();
          packGroup->push_back(syncPackKVEntry(seq, key, value));
        }
        else {
          packGroup->push_back(syncPackKVEntry((*packGroup)[0].phyKey, key, value));
          if (packGroup->size() == options_.maxPackNum) {
            kvssd::Slice pack_key;
            kvssd::Slice pack_val;
            IDXWriteBatch *index_batch ;
            if (options_.indexType == LSM) {
              index_batch = NewIDXWriteBatchLSM();
            }
            else if (options_.indexType == LSMOPT) {
              index_batch = NewIDXWriteBatchLSM();
            }
            else if (options_.indexType == ROCKS) {
              index_batch = NewIDXWriteBatchRocks();
            }
            else if (options_.indexType == BTREE) {
              index_batch = NewIDXWriteBatchBTree();
            }
            else if (options_.indexType == BASE) {
              index_batch = NewIDXWriteBatchBase();
            }
            else if (options_.indexType == INMEM) {
              index_batch = NewIDXWriteBatchInmem();
            }

            do_pack_KVs(packGroup, pack_key, pack_val, index_batch);
            RecordTick(options_.statistics.get(), PACK_CNT);
            RecordTick(options_.statistics.get(), PACK_RECORD_CNT, packGroup->size());
            RecordTick(options_.statistics.get(), PACK_BYTES, pack_val.size());

            // sync write 1-data, 2-index
            kvd_->kv_store(&pack_key, &pack_val);
            key_idx_[0]->Write(index_batch); // pack only support single index tree

            // clean up pack buffer
            free((void*)pack_key.data());
            free((void*)pack_val.data());

            // erase group in the sync queue
            sq_[sq_shard_id].erase (options.packID);  
          }
        }
      }
      return Status();
    }
    
    if (value.size() < options_.packThres && (!options_.packThreadsDisable)) { // packed value
      int size = get_phyKV_size(key, value);
      // Monitor mon;
      // packKVEntry *item = new packKVEntry(size, key, value, &mon);

      packKVEntry *item = new packKVEntry(size, key, value, nullptr);
      while (pack_q_.size_approx() > options_.packQueueDepth) {
        pack_q_wait_.reset();
        pack_q_wait_.wait();
      }
      pack_q_.enqueue(item);
      // mon.wait();
    }

    else { // unpacked put
      kvssd::Slice put_key(key.data(), key.size());
      kvssd::Slice put_val(value.data(), value.size());

      if (options_.bfHotKeyNum > 0) { // dynamic build hotkey filter
        InsertEntryBloomFilter(key);
      }

      // sync write 1-data, 2-index
      // Monitor mon;
      // kvd_->kv_store_async(&put_key, &put_val, on_io_complete, &mon);
      // // index write
      // int idx_id = (options_.indexNum == 1) ? 0 : MurmurHash64A(key.data(), key.size(), 0)%options_.indexNum;
      // key_idx_[idx_id]->Put(key);
      
      // mon.wait(); // wait data I/O done

      // sync write 1-data, 2-index
      kvd_->kv_store(&put_key, &put_val);
      int idx_id = (options_.indexNum == 1) ? 0 : MurmurHash64A(key.data(), key.size(), 0)%options_.indexNum;
      key_idx_[idx_id]->Put(key);

    }
  }

  
  return Status();
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  // delete in-memory cache
  std::string skey(key.data(), key.size());
  erase_cache_entry(skey);
  erase_buffer_entry(skey);

  RecordTick(options_.statistics.get(), REQ_DEL);
  total_record_count_.fetch_sub(1, std::memory_order_relaxed);

  kvssd::Slice del_key(key.data(), key.size());
	kvd_->kv_delete(&del_key);
  int idx_id = (options_.indexNum == 1) ? 0 : MurmurHash64A(key.data(), key.size(), 0)%options_.indexNum;
  key_idx_[idx_id]->Delete(key);

  if (options_.bfHotKeyNum > 0) { // dynamic build hotkey filter
    DeleteEntryBloomFilter(key);
  }
  return Status();
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  int batch_size = updates->Size();
  IDXWriteBatch **idx_batch = new IDXWriteBatch*[options_.indexNum];
  Monitor *mons = new Monitor[batch_size];
  for (int i = 0; i < options_.indexNum; i++) {
    if (options_.indexType == LSM) {
      idx_batch[i] = NewIDXWriteBatchLSM();
    }
    else if (options_.indexType == LSMOPT) {
      idx_batch[i] = NewIDXWriteBatchLSM();
    }
    else if (options_.indexType == ROCKS) {
      idx_batch[i] = NewIDXWriteBatchRocks();
    }
    else if (options_.indexType == BTREE) {
      idx_batch[i] = NewIDXWriteBatchBTree();
    }
    else if (options_.indexType == BASE) {
      idx_batch[i] = NewIDXWriteBatchBase();
    }
    else if (options_.indexType == INMEM) {
      idx_batch[i] = NewIDXWriteBatchInmem();
    }
  }
  
  for (int i = 0; i < batch_size; i++) {
    kvssd::Slice put_key(updates->batch_[i].first.data(), updates->batch_[i].first.size());
    kvssd::Slice put_val(updates->batch_[i].second.data(), updates->batch_[i].second.size());
    // kvd_->kv_store_async(&put_key, &put_val, on_io_complete, &mons[i]);

    // Slice db_key(updates->batch_[i].first.data(), updates->batch_[i].first.size());
    // int idx_id = (options_.indexNum == 0) ? 0 : MurmurHash64A(updates->batch_[i].first.data(), updates->batch_[i].first.size(), 0);
    // idx_batch[idx_id]->Put(db_key);

    kvd_->kv_store(&put_key, &put_val);
    Slice db_key(updates->batch_[i].first.data(), updates->batch_[i].first.size());
    int idx_id = (options_.indexNum == 0) ? 0 : MurmurHash64A(updates->batch_[i].first.data(), updates->batch_[i].first.size(), 0);
    idx_batch[idx_id]->Put(db_key);
  }
  
  for (int i = 0; i < options_.indexNum; i++) {
    key_idx_[i]->Write(idx_batch[i]);
    delete idx_batch[i];
  }
  for(int i = 0; i < batch_size; i++) {
    mons[i].wait();
  }
  delete [] mons;
  delete [] idx_batch;

  return Status();
}

Status DBImpl::Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value) {
  RecordTick(options_.statistics.get(), REQ_GET);
  //printf("KVRangeDB Get %s\n", std::string(key.data(), key.size()).c_str());

  // check range filter if needed
  if (rf_ && (!rf_->KeyMayMatch(key))) {
    RecordTick(options_.statistics.get(), FILTER_POINT_NEGATIVE);
    return Status().NotFound(Slice()); 
  }
  RecordTick(options_.statistics.get(), FILTER_POINT_POSITIVE);
  
  // read in-memory cache
  std::string skey(key.data(), key.size());
  Cache::Handle *h = read_cache(skey, value);
  if (h != NULL) { // hit in cache
      release_cache_entry(h);
      return Status();
  }

  if (options_.manualCompaction && hot_keys_training_cnt_.load() < options_.hotKeyTrainingNum) { // capture hot query
    {
      std::unique_lock<std::mutex> lock(seq_mutex_);
      hot_keys_[std::string(key.data(), key.size())]++;
    }
    hot_keys_training_cnt_.fetch_add(1);
    return Status();
  }
  
  if (options.hint_packed == PACKED) { // definitely packed records
    // index lookup
    Slice lkey(key.data(), key.size());
    std::string pkey;
    bool idx_found = key_idx_[0]->Get(lkey, pkey);
    if (!idx_found) { // definitely not found
      return Status().NotFound(Slice());
    }
    else if (pkey.size() == 0) { // not packing, normally should fall into this path
      fprintf(stderr, "[kv_get] packed hint, key not packed, log_key: %s \n", key.data());
      return Status().NotFound(Slice());
      
    } else { // packing with key translation
      // check write buffer before reaching device if key exist
      Cache::Handle *bh = read_buffer(skey, value);
      if (bh != NULL) { // hit in write buffer
          release_buffer_entry(bh);
          return Status();
      }

      kvssd::Slice get_key(pkey);
      char *vbuf;
      int vlen;
      int ret = kvd_->kv_get(&get_key, vbuf, vlen);
      assert (ret == 0);
      bool found = do_unpack_KVs(vbuf, vlen, key, value);
      assert (found == true);
      free(vbuf);

      // insert to in-memory cache
      const Slice val(value->data(), value->size());
      h = insert_cache(skey, val);
      release_cache_entry(h);
      return Status();
    }
  } else if (options.hint_packed == UNPACKED) { // definitely unpacked records
    // check write buffer before reaching device if key exist
    Cache::Handle *bh = read_buffer(skey, value);
    if (bh != NULL) { // hit in write buffer
        release_buffer_entry(bh);
        return Status();
    }

    kvssd::Slice get_key(key.data(), key.size());
    char *vbuf;
    int vlen;
    int ret = kvd_->kv_get(&get_key, vbuf, vlen);
    if (ret == 0) {
      value->append(vbuf, vlen);
      free(vbuf);
      // insert to in-memory cache
      const Slice val(value->data(), value->size());
      h = insert_cache(skey, val);
      release_cache_entry(h);
      return Status();
    }
    else {
      free(vbuf);
      return Status().NotFound(Slice());
    }
  } else if (options.hint_packed == LIKELY_PACKED) { // likely packed records
    // index lookup
    Slice lkey(key.data(), key.size());
    std::string pkey;
    bool idx_found = key_idx_[0]->Get(lkey, pkey);
    if (!idx_found) { // definitely not found
      return Status().NotFound(Slice());
    }

    // check write buffer before reaching device if key exist
    Cache::Handle *bh = read_buffer(skey, value);
    if (bh != NULL) { // hit in write buffer
        release_buffer_entry(bh);
        return Status();
    }
    if (pkey.size() == 0) { // not packing
      kvssd::Slice get_key(key.data(), key.size());
      char *vbuf;
      int vlen;
      int ret = kvd_->kv_get(&get_key, vbuf, vlen);
      if (ret == 0) {
        value->append(vbuf, vlen);
        free(vbuf);
        // insert to in-memory cache
        const Slice val(value->data(), value->size());
        h = insert_cache(skey, val);
        release_cache_entry(h);
        return Status();
      } else {
        free(vbuf);
        return Status().NotFound(Slice());
      }
      
    } else { // packing with key translation
      kvssd::Slice get_key(pkey);
      char *vbuf;
      int vlen;
      int ret = kvd_->kv_get(&get_key, vbuf, vlen);
      assert(ret == 0);
      bool found = do_unpack_KVs(vbuf, vlen, key, value);
      assert(found == true);
      free(vbuf);

      // insert to in-memory cache
      const Slice val(value->data(), value->size());
      h = insert_cache(skey, val);
      release_cache_entry(h);
      return Status();
    }
  } else if (options.hint_packed == LIKELY_UNPACKED) { // likely unpacked records
    // check write buffer before reaching device if key exist
    Cache::Handle *bh = read_buffer(skey, value);
    if (bh != NULL) { // hit in write buffer
        release_buffer_entry(bh);
        return Status();
    }

    kvssd::Slice get_key(key.data(), key.size());
    char *vbuf;
    int vlen;
    int ret = kvd_->kv_get(&get_key, vbuf, vlen);
    if (ret == 0) {
      value->append(vbuf, vlen);
      free(vbuf);
      // insert to in-memory cache
      const Slice val(value->data(), value->size());
      h = insert_cache(skey, val);
      release_cache_entry(h);
      return Status();
    }
    else {
      free(vbuf);
      // index lookup
      Slice lkey(key.data(), key.size());
      std::string pkey;
      bool idx_found = key_idx_[0]->Get(lkey, pkey);
      if (!idx_found) { // definitely not found
        return Status().NotFound(Slice());
      } else {
        assert(pkey.size() > 0);
        kvssd::Slice get_key(pkey);
        char *vbuf;
        int vlen;
        int ret = kvd_->kv_get(&get_key, vbuf, vlen);
        assert(ret == 0);
        bool found = do_unpack_KVs(vbuf, vlen, key, value);
        assert(found == true);
        free(vbuf);
            
        // insert to in-memory cache
        const Slice val(value->data(), value->size());
        h = insert_cache(skey, val);
        release_cache_entry(h);
        return Status();
      }
    }
  }

  // DEFAULT (fall over)
  // bloom filter check for hot key
  bool hotkey_found = CheckBloomFilter(key);
  if(hotkey_found) { // filter hit, highly possible hot key
    // check write buffer before reaching device if key exist
    Cache::Handle *bh = read_buffer(skey, value);
    if (bh != NULL) { // hit in write buffer
        release_buffer_entry(bh);
        return Status();
    }

    kvssd::Slice get_key(key.data(), key.size());
    char *vbuf;
    int vlen;
    int ret = kvd_->kv_get(&get_key, vbuf, vlen);
    
    if (ret == 0) {
      value->append(vbuf, vlen);
      free(vbuf);

      // insert to in-memory cache
      const Slice val(value->data(), value->size());
      h = insert_cache(skey, val);
      release_cache_entry(h);
      return Status();
    }
    else { // possible filter false positive
      free(vbuf);
      // fall through
    }
  } 
  // filter miss, definitely not hot key
  // index lookup
  Slice lkey(key.data(), key.size());
  std::string pkey;
  bool idx_found = key_idx_[0]->Get(lkey, pkey);
  if (!idx_found) { // definitely not found
    return Status().NotFound(Slice());
  }
  // check write buffer before reaching device if key exist
  Cache::Handle *bh = read_buffer(skey, value);
  if (bh != NULL) { // hit in write buffer
      release_buffer_entry(bh);
      return Status();
  }

  if(pkey.size() == 0) { // not possible, since filter miss or logical key already checked
    // uint64_t ino = be64toh(*((uint64_t*)key.data())) >> 8;
    // fprintf(stderr, "[kv_get] default hint, filter_found: %d, cold key not packed, log_key: %llu, %s\n",hotkey_found, ino, key.data()+sizeof(uint64_t));
    /***** corner case we might need to check again *****/
    kvssd::Slice get_key(key.data(), key.size());
    char *vbuf;
    int vlen;
    int ret = kvd_->kv_get(&get_key, vbuf, vlen);
    
    if (ret == 0) {
      value->append(vbuf, vlen);
      free(vbuf);

      // insert to in-memory cache
      const Slice val(value->data(), value->size());
      h = insert_cache(skey, val);
      release_cache_entry(h);
      return Status();
    }
    else { // possible filter false positive
      free(vbuf);
      return Status().NotFound(Slice());
    }

  } else { // read packed physical record
    kvssd::Slice get_key(pkey);

    char *vbuf;
    int vlen;
    int ret = kvd_->kv_get(&get_key, vbuf, vlen);
    assert(ret == 0);
    bool found = do_unpack_KVs(vbuf, vlen, key, value);
    assert(found == true);
    free(vbuf);
        
    // insert to in-memory cache
    const Slice val(value->data(), value->size());
    h = insert_cache(skey, val);
    release_cache_entry(h);
    return Status();
  }
  // not reachable
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  return NewDBIterator(this, options);
}

typedef struct {
  std::mutex *m;
  int fetch_cnt;
  int fetch_num;
  Monitor *mon;
} Compaction_context;

static void on_compact_get_complete (void* args) {
  Compaction_context *compact_ctx = (Compaction_context *)args;
  std::mutex *m = compact_ctx->m;
  {
    std::unique_lock<std::mutex> lck(*m);
    if (compact_ctx->fetch_cnt++ == compact_ctx->fetch_num-1)
      compact_ctx->mon->notify();
  }
}

static void on_compact_del_complete (void* args) {
  Compaction_context *compact_ctx = (Compaction_context *)args;
  std::mutex *m = compact_ctx->m;
  {
    std::unique_lock<std::mutex> lck(*m);
    if (compact_ctx->fetch_cnt++ == compact_ctx->fetch_num-1)
      compact_ctx->mon->notify();
  }
}

static void on_compact_put_complete (void* args) {
  Compaction_context *compact_ctx = (Compaction_context *)args;
  std::mutex *m = compact_ctx->m;
  {
    std::unique_lock<std::mutex> lck(*m);
    if (compact_ctx->fetch_cnt++ == compact_ctx->fetch_num-1)
      compact_ctx->mon->notify();
  }
}

/* IN-Progress */
// void DBImpl::BGCompaction() {
//   const auto timeWindow = std::chrono::seconds(options_.bgCompactionInterval);
//   int scanLen = options_.bgCompactionScanLength;
  
//   bool wrap_around = true;
//   std::string next_seek_key;
//   while(true) {
//     auto start = std::chrono::steady_clock::now();
//     const ReadOptions options;
//     IDXIterator *it_ = key_idx_[0]->NewIterator(options);
//     // heavy lifting
//     if (wrap_around) {
//       it_->SeekToFirst();
//       wrap_around = false;
//     }
//     else {
//       it_->Seek(next_seek_key);
//     }

//     for (int i = 0; i < scanLen; i++) {
//       if (!it_->Valid()) {
//         // reach the end of the index
//         wrap_around = true;
//         next_seek_key = "";
//       }
      
//       it_->Next();
//     }
//     if (!wrap_around) {
//       Slice next_seek_key_slice = it_->key();
//       next_seek_key = std::string(next_seek_key_slice.data(), next_seek_key_slice.size());
//     }
//     delete it_;

//     auto end = std::chrono::steady_clock::now();
//     auto elapsed = end - start;

//     auto timeToWait = timeWindow - elapsed;
//     if(timeToWait > std::chrono::milliseconds::zero())
//     {
//         std::this_thread::sleep_for(timeToWait);
//     }

//   }

// }

#define COMPACT_THD 16
#define COMPACT_SIZE 16
#define COMPACT_THRES 10000000 // 10 Million
#define COMPACT_THRES_GUARD 100000 // 100 K

typedef struct {
  int klen;
  char *kdata;
  int vlen;
  char *vdata;
} kv_record;

static void do_pack_KVs (uint64_t seq, std::vector<kv_record>& pack_list, 
        kvssd::Slice& pack_key, kvssd::Slice& pack_val, IDXWriteBatch *index_batch) {
    int pack_size = 0;
    for (int i = 0; i < pack_list.size(); i++) {
      pack_size = pack_size + (pack_list[i].klen+pack_list[i].vlen+sizeof(uint8_t)+sizeof(uint32_t));
    }
    char* pack_key_str = (char*) malloc(sizeof(uint64_t));
    char* pack_val_str = (char*) malloc(pack_size);
    // key
    *((uint64_t*)pack_key_str) = seq;
    pack_key = kvssd::Slice (pack_key_str, sizeof(uint64_t));
    Slice pkey(pack_key_str, sizeof(uint64_t));

    // value
    char *p = pack_val_str;
    for (int i = 0; i < pack_list.size(); i++) {
      Slice lkey(pack_list[i].kdata, pack_list[i].klen);
      index_batch->Put(lkey, pkey);

      *((uint8_t*)p) = pack_list[i].klen;
      p += sizeof(uint8_t);
      memcpy(p, pack_list[i].kdata, pack_list[i].klen);
      p += pack_list[i].klen;
      *((uint32_t*)p) = pack_list[i].vlen;
      p += sizeof(uint32_t);
      memcpy(p, pack_list[i].vdata, pack_list[i].vlen);
      p += pack_list[i].vlen;

    }
    assert((int)(p -pack_val_str) == pack_size);
    pack_val = kvssd::Slice (pack_val_str, pack_size);

    return;
};

void DBImpl::DoBGCompact(std::vector<std::string>* klist, int offset, int size) {
  std::vector<kv_record> pack_list;
  int pack_cnt = 0;
  for (int i = offset; i < offset+size; i++) {
    kvssd::Slice get_key((*klist)[i]);
    kv_record item;
    item.klen = (*klist)[i].size();
    item.kdata = (char*)((*klist)[i].data());
    int ret = kvd_->kv_get(&get_key, item.vdata, item.vlen);
    pack_list.push_back(item);

    if (++pack_cnt == COMPACT_SIZE || i == (offset+size-1)) {
      // create seq number
      uint64_t seq;
      seq = get_new_seq();
      
      // do packing
      kvssd::Slice pack_key;
      kvssd::Slice pack_val;
      IDXWriteBatch *index_batch ;
      if (options_.indexType == LSM) {
        index_batch = NewIDXWriteBatchLSM();
      }
      else if (options_.indexType == LSMOPT) {
        index_batch = NewIDXWriteBatchLSM();
      }
      else if (options_.indexType == ROCKS) {
        index_batch = NewIDXWriteBatchRocks();
      }
      else if (options_.indexType == BTREE) {
        index_batch = NewIDXWriteBatchBTree();
      }
      else if (options_.indexType == BASE) {
        index_batch = NewIDXWriteBatchBase();
      }
      else if (options_.indexType == INMEM) {
        index_batch = NewIDXWriteBatchInmem();
      }

      do_pack_KVs(seq, pack_list, pack_key, pack_val, index_batch);

      // sync write 1-data, 2-index
      kvd_->kv_store(&pack_key, &pack_val);
      key_idx_[0]->Write(index_batch); // pack only support single index tree
      
      // clean up
      for (int i = 0; i < pack_list.size(); i++) {
        kvssd::Slice del_key(pack_list[i].kdata, pack_list[i].klen);
        kvd_->kv_delete(&del_key);
        free(pack_list[i].vdata);
      }
      free((char*) pack_key.data());
      free((char*) pack_val.data());
      delete index_batch;

      // reset pack list
      pack_cnt = 0;
      pack_list.clear();
    }
  }
}

void DBImpl::BGCompaction() {
  const auto timeWindow = std::chrono::seconds(options_.bgCompactionInterval);
  
  std::string next_seek_key;
  const uint64_t pack_thres = COMPACT_THRES; 
  std::thread *compact_thrd[COMPACT_THD]; // compact threads
  while(bg_compact_shutdown_.load(std::memory_order_relaxed) != true) {
    auto start = std::chrono::steady_clock::now();

    if (total_record_count_.load(std::memory_order_relaxed) - packed_record_count_.load(std::memory_order_relaxed) > (COMPACT_THRES + COMPACT_THRES_GUARD)) {
      auto timeBGCStart = std::chrono::system_clock::now();
      printf("start BG compacting, total_keys: %lu, compacted keys: %lu\n", 
      total_record_count_.load(std::memory_order_relaxed), packed_record_count_.load(std::memory_order_relaxed));

      int key_cnt = 0, last_key_cnt = 0;
      std::vector<std::string> pack_key_list;
      uint64_t pack_cnt = 0;
      const ReadOptions rdopts;
      
      // scan keys for packing
      IDXIterator *it_ = key_idx_[0]->NewIterator(rdopts);
      if (next_seek_key.empty()) {
        it_->SeekToFirst();
      } else {
        Slice seekKey(next_seek_key);
        it_->Seek(seekKey);
      }
      while(it_->Valid() && pack_cnt++ < pack_thres) {
        Slice lkey = it_->key();
        std::string key(lkey.data(), lkey.size());
        pack_key_list.push_back(key);
        it_->Next();
      }
      // assign next compaction seek key
      Slice lkey = it_->key();
      next_seek_key = lkey.ToString();
      delete it_;

      // schedule threads for compaction
      printf("BGCompact: total scan keys: %d, schedule threads to compact, next seek key: %s\n", pack_key_list.size(), next_seek_key.c_str());
      int perThreadCnt = pack_key_list.size()/COMPACT_THD;
      int offset = 0;
      int size = 0;
      for (int i = 0; i< COMPACT_THD; i++) {
          if (i == COMPACT_THD-1) size = pack_key_list.size() - offset;
          else size = perThreadCnt;
          compact_thrd[i] = new std::thread(&DBImpl::DoBGCompact, this, &pack_key_list, offset, size);
          offset += perThreadCnt;
      }
      for (int i = 0; i< COMPACT_THD; i++) {
          compact_thrd[i]->join();
          delete compact_thrd[i];
      }
      packed_record_count_.fetch_add(pack_key_list.size(), std::memory_order_relaxed);

      auto timeBGCEnd = std::chrono::system_clock::now();
      std::chrono::duration<double> BGCDuration = (timeBGCEnd - timeBGCStart);
      printf("end BG compacting, total_keys: %lu, compacted keys: %lu, elapsed: %.3f (sec)\n", 
      total_record_count_.load(std::memory_order_relaxed), packed_record_count_.load(std::memory_order_relaxed), BGCDuration.count());
    }

    auto end = std::chrono::steady_clock::now();
    auto elapsed = end - start;

    auto timeToWait = timeWindow - elapsed;
    if(timeToWait > std::chrono::milliseconds::zero())
    {
        std::this_thread::sleep_for(timeToWait);
    }

  }

}

void DBImpl::ManualCompaction() {
  options_.packDequeueTimeout = 60000; // increase dequeue timeout
  int pack_cnt = 0;
  int unpack_cnt = 0;
  int key_cnt = 0, last_key_cnt = 0;
  const int batch_size = 256;
  const ReadOptions options;
  IDXIterator *it_ = key_idx_[0]->NewIterator(options);
  it_->SeekToFirst();
  while(it_->Valid()) {

    // batch compact
    std::vector<std::string> pack_key_batch;
    std::vector<kvssd::Slice> pack_fetch_key_list;
    std::vector<std::string> unpack_key_batch;
    std::vector<kvssd::Slice> unpack_fetch_key_list;
    for (int i = 0; i < batch_size&&it_->Valid(); i++, it_->Next(), key_cnt++) {
      Slice lkey = it_->key();
      Slice pkey = it_->pkey();
      auto kit= hot_keys_.find(std::string(lkey.data(), lkey.size()));
      if (pkey.size() == 0 && kit == hot_keys_.end()) {
        pack_key_batch.push_back(std::string(lkey.data(), lkey.size()));
        pack_fetch_key_list.push_back(pack_key_batch[pack_key_batch.size()-1]);
      }
      else if (pkey.size() > 0 && kit != hot_keys_.end() && kit->second > 1) { 
        unpack_key_batch.push_back(std::string(lkey.data(), lkey.size()));
        unpack_fetch_key_list.push_back(unpack_key_batch[unpack_key_batch.size()-1]);
      }
    }
    assert(pack_fetch_key_list.size() == pack_key_batch.size());
    assert(unpack_fetch_key_list.size() == unpack_key_batch.size());

    int fetch_num = pack_key_batch.size();
    int unpack_fetch_num = unpack_key_batch.size();
    if (fetch_num==0 && unpack_fetch_num==0) continue;

    Monitor mon_unpack_keys;
    std::mutex m_unpack;
    char **vbuf_list_unpack = new char*[unpack_fetch_num];
    uint32_t *actual_vlen_list_unpack = new uint32_t[unpack_fetch_num];
    Compaction_context *ctx_unpack = new Compaction_context {&m_unpack, 0, unpack_fetch_num, &mon_unpack_keys};
    for (int i = 0; i < unpack_fetch_num; i++) {
      kvssd::Async_get_context *io_ctx = new kvssd::Async_get_context (kvd_, vbuf_list_unpack[i], actual_vlen_list_unpack[i], (void *)ctx_unpack);
      kvd_->kv_get_async(&unpack_fetch_key_list[i], on_compact_get_complete, (void*) io_ctx);
    }
    

    Monitor mon_pack_keys;
    std::mutex m_pack;
    char **vbuf_list = new char*[fetch_num];
    uint32_t *actual_vlen_list = new uint32_t[fetch_num];
    Compaction_context *ctx_pack = new Compaction_context {&m_pack, 0, fetch_num, &mon_pack_keys};
    for (int i = 0; i < fetch_num; i++) {
      kvssd::Async_get_context *io_ctx = new kvssd::Async_get_context (kvd_, vbuf_list[i], actual_vlen_list[i], (void *)ctx_pack);
      kvd_->kv_get_async(&pack_fetch_key_list[i], on_compact_get_complete, (void*) io_ctx);
    }

    if(unpack_fetch_num) mon_unpack_keys.wait();
    if(fetch_num) mon_pack_keys.wait();

    Monitor mon_p;
    std::mutex m_p;
    std::string *hot_vals = new std::string[unpack_fetch_num];
    std::vector<kvssd::Slice> hot_vals_list;
    Compaction_context *ctx_p = new Compaction_context {&m_p, 0, unpack_fetch_num, &mon_p};
    for (int i = 0; i < unpack_fetch_num; i++) {
      Slice lkey (unpack_key_batch[i]);
      bool found = do_unpack_KVs(vbuf_list_unpack[i], actual_vlen_list_unpack[i], lkey, &hot_vals[i]);
      assert(found);
      hot_vals_list.push_back(hot_vals[i]);

      kvd_->kv_store_async(&unpack_fetch_key_list[i], &hot_vals_list[i], on_compact_put_complete, (void*) ctx_p);
      unpack_cnt++;
    }

    Monitor mon_d;
    std::mutex m_d;
    Compaction_context *ctx_d = new Compaction_context {&m_d, 0, fetch_num, &mon_d};
    for (int i = 0; i < fetch_num; i++) {
      Slice lkey (pack_key_batch[i]);
      Slice cold_val (vbuf_list[i], actual_vlen_list[i]);
      int size = get_phyKV_size(lkey, cold_val);
      packKVEntry *item = new packKVEntry(size, lkey, cold_val, nullptr);
      while (pack_q_.size_approx() > options_.packQueueDepth) {
        pack_q_wait_.reset();
        pack_q_wait_.wait();
      }
      pack_q_.enqueue(item);
      pack_cnt++;
      kvd_->kv_delete_async(&pack_fetch_key_list[i], on_compact_del_complete, (void*) ctx_d);
      free(vbuf_list[i]);
    }

    if(unpack_fetch_num) mon_p.wait();
    if(fetch_num) mon_d.wait();

    // clean up
    for (int i = 0; i < unpack_fetch_num; i++) {
      free(vbuf_list_unpack[i]);
    }
    delete [] vbuf_list_unpack;
    delete [] actual_vlen_list_unpack;
    delete [] hot_vals;

    delete [] vbuf_list;
    delete [] actual_vlen_list;
    delete ctx_pack;
    delete ctx_unpack;
    delete ctx_d;
    delete ctx_p;

    if (key_cnt - last_key_cnt >= 1000000) {
      printf("[ManualCompaction] total KVs %d, packed KVs %d, unpack KVs %d\n", key_cnt, pack_cnt, unpack_cnt);
      last_key_cnt = key_cnt;
    }
  }
  printf("[ManualCompaction] total KVs %d, packed KVs %d, unpack KVs %d\n", key_cnt, pack_cnt, unpack_cnt);
  delete it_;
}

void DBImpl::BuildBloomFilter() {
  bf_.clear();
  CounterBloomFilter bf(options_.bfCellsPerKey, options_.bfCellWidth);
  int key_cnt = 0;
  std::vector<Slice> tmp_keys;
  for (auto it = hot_keys_.begin(); it != hot_keys_.end(); ++it) {
    if(it->second > 1) {
      tmp_keys.push_back(Slice(it->first));
      key_cnt++;
    }
  }
  bf.CreateFilter(&tmp_keys[0], key_cnt, &bf_);
  printf("Bloom Filter created, %d keys, %d bytes\n", key_cnt, bf_.size());
}

void DBImpl::CreateEmtpyBloomFilter(int key_cnt) {
  bf_.clear();
  CounterBloomFilter bf(options_.bfCellsPerKey, options_.bfCellWidth);
  
  bf.CreateEmptyFilter(key_cnt, &bf_);
  printf("Empty Bloom Filter created, %d keys, %d bytes\n", key_cnt, bf_.size());
}

void DBImpl::InsertEntryBloomFilter(const Slice& key) {
  // uint64_t ino = be64toh(*((uint64_t*)key.data())) >> 8;
  // fprintf(stderr, "BF insert :%llu,  %s\n", ino, key.data()+sizeof(uint64_t));
  CounterBloomFilter bf(options_.bfCellsPerKey, options_.bfCellWidth);
  {
    std::unique_lock<std::mutex> lock(hk_mutex_);
    bf.InsertEntry(key, &bf_);
  }
}

void DBImpl::DeleteEntryBloomFilter(const Slice& key) {
  // uint64_t ino = be64toh(*((uint64_t*)key.data())) >> 8;
  // fprintf(stderr, "BF insert :%llu,  %s\n", ino, key.data()+sizeof(uint64_t));
  CounterBloomFilter bf(options_.bfCellsPerKey, options_.bfCellWidth);
  {
    std::unique_lock<std::mutex> lock(hk_mutex_);
    bf.DeleteEntry(key, &bf_);
  }
}

bool DBImpl::CheckBloomFilter(const Slice& key) {
  if (bf_.size()) {
    // uint64_t ino = be64toh(*((uint64_t*)key.data())) >> 8;
    // fprintf(stderr, "BF check :%llu,  %s\n", ino, key.data()+sizeof(uint64_t));
    CounterBloomFilter bf(options_.bfCellsPerKey, options_.bfCellWidth);
    return bf.KeyMayMatch(key, bf_);
  }
  else { // no filter
    return true;
  }
}

void DBImpl::BuildRangeFilter() {
  assert(rf_);

  struct timespec ts_start;
  struct timespec ts_end;
  uint64_t elapsed;
  clock_gettime(CLOCK_MONOTONIC, &ts_start);

  std::string filter_name = rf_->GenFilterName();
  std::ifstream f(filter_name.c_str());
  if(f.good()) {
    rf_->LoadFilter(filter_name);

    clock_gettime(CLOCK_MONOTONIC, &ts_end);
    elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
    static_cast<uint64_t>(ts_end.tv_nsec) -
    static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
    static_cast<uint64_t>(ts_start.tv_nsec);
    printf("Range Filter loaded from file, elapsed %.3f\n", (static_cast<double>(elapsed) / 1000000000.));
  }
  else {
    const ReadOptions options;
    int key_cnts = 0;
    IDXIterator *it_ = key_idx_[0]->NewIterator(options);
    it_->SeekToFirst();
    while(it_->Valid()) {
      rf_->InsertItem(it_->key());
      it_->Next();
      key_cnts++;
    }

    clock_gettime(CLOCK_MONOTONIC, &ts_end);
    elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
    static_cast<uint64_t>(ts_end.tv_nsec) -
    static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
    static_cast<uint64_t>(ts_start.tv_nsec);
    printf("Range Filter created, elapsed %.3f, #keys %d\n", (static_cast<double>(elapsed) / 1000000000.), key_cnts);
  }
}

Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr) {

  *dbptr = NULL;

  DB *db = new DBImpl(options, dbname);
  *dbptr = db;
  return Status(Status::OK());
}

}  // namespace kvrangedb

