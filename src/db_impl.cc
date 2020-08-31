/******* kvrangedb *******/
/* db_impl.cc
* 07/23/2019
* by Mian Qin
*/
#include <mutex>
#include <condition_variable>
#include "kvrangedb/db.h"
#include "kvrangedb/iterator.h"
#include "kvrangedb/write_batch.h"
#include "db_impl.h"
#include "db_iter.h"
#include "hash.h"

extern int hitCnt;
extern double hitCost;
extern double missCost;
extern double hitNextCost;
extern double missNextCost;
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
  pack_threads_num(options.packThreadsNum) {
  kvd_ = new kvssd::KVSSD(dbname.c_str());
  for (int i = 0; i < options.indexNum; i++) {
    std::string indexName = std::to_string(i);
    if (options.indexType == LSM) {
      key_idx_[i] = NewLSMIndex(options, kvd_, indexName);
    }
    else if (options.indexType == LSMOPT) {
      key_idx_[i] = NewLSMIndex(options, kvd_, indexName);
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
  pack_threads_ = new std::thread*[pack_threads_num];
  thread_m_ = new std::mutex[pack_threads_num];
  shutdown_ = new bool[pack_threads_num];
  for (int i = 0; i < pack_threads_num; i++) {
    shutdown_[i] = false;
    pack_threads_[i] = new std::thread(&DBImpl::processQ, this, i);
  }

}

DBImpl::~DBImpl() {
  if (options_.cleanIndex) {
    for (int i = 0; i < options_.indexNum; i++) {
      std::string meta_name = std::to_string(i)+"/CURRENT";
      kvssd::Slice del_key_lsm(meta_name);
      kvd_->kv_delete(&del_key_lsm);
    }
  }

  // shutdown packing threads
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

  for (int i = 0; i < options_.indexNum; i++)
    delete key_idx_[i];
	delete kvd_;
  printf("hitCnt = %d\n", hitCnt);
  printf("hitCost = %.3f, missCost = %.3f\n", hitCost, missCost);
  printf("hitNextCost = %.3f, missNextCost = %.3f\n", hitNextCost, missNextCost);
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

      delete kvs[i]; // de-allocate KV buffer;
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
  while (true && (!shutdown)) {
    // check thread shutdown
    {
      std::unique_lock<std::mutex> lck (thread_m_[id]);
      if (shutdown_[id] == true && (!ready_to_shutdown)) {
          // clean up
          sleep(1); // wait all enqueue done
          ready_to_shutdown = true;
      }
    }
    // dequeue
    std::vector<packKVEntry*> kvs;
    int pack_size = dequeue_bulk_timed(pack_q_, kvs, options_.maxPackNum, options_.packSize, options_.packDequeueTimeout);
    if (kvs.size()) {
      uint64_t seq;
      {
          std::unique_lock<std::mutex> lock(seq_mutex_);
          seq = sequence_++;
      }
      
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
      Monitor mon;
      kvd_->kv_store_async(&pack_key, &pack_val, on_io_complete, &mon);
      
      // index write
      key_idx_[0]->Write(index_batch); // pack only support single index tree

      mon.wait(); // wait data I/O done
      
      // clean up
      free((char*) pack_key.data());
      free((char*) pack_val.data());
      if (pack_q_.size_approx() <= options_.packQueueDepth) 
      pack_q_wait_.notifyAll();
    }
    else { // check shutdown
        if (ready_to_shutdown && pack_q_.size_approx() == 0) {
            shutdown = true;
            break;
        }
    }
  }
}

inline int get_phyKV_size(const Slice& key, const Slice& value) {
  return key.size()+value.size()+sizeof(uint8_t)+sizeof(uint32_t);
}

Status DBImpl::Put(const WriteOptions& options,
                     const Slice& key,
                     const Slice& value) {

  // smaller values
  if (value.size() < options_.packThres) {
    int size = get_phyKV_size(key, value);
    packKVEntry *item = new packKVEntry(size, key, value);
    while (pack_q_.size_approx() > options_.packQueueDepth) {
      pack_q_wait_.reset();
      pack_q_wait_.wait();
    }
    pack_q_.enqueue(item);
  }

  else {
    kvssd::Slice put_key(key.data(), key.size());
    kvssd::Slice put_val(value.data(), value.size());
    Monitor mon;
    kvd_->kv_store_async(&put_key, &put_val, on_io_complete, &mon);
    // index write
    int idx_id = (options_.indexNum == 0) ? 0 : MurmurHash64A(key.data(), key.size(), 0)%options_.indexNum;
    key_idx_[idx_id]->Put(key);
    
    mon.wait(); // wait data I/O done
  }
  return Status();
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  kvssd::Slice del_key(key.data(), key.size());
	kvd_->kv_delete(&del_key);
  int idx_id = (options_.indexNum == 0) ? 0 : MurmurHash64A(key.data(), key.size(), 0)%options_.indexNum;
  key_idx_[idx_id]->Delete(key);
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
    kvd_->kv_store_async(&put_key, &put_val, on_io_complete, &mons[i]);

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
  if (options.hint_packed == 2) { // small value
    // index lookup
    Slice lkey(key.data(), key.size());
    std::string pkey;
    key_idx_[0]->Get(lkey, pkey);
    kvssd::Slice get_key(pkey);

    char *vbuf;
    int vlen;
    int ret = kvd_->kv_get(&get_key, vbuf, vlen);

    if (ret == 0) {
      bool found = do_unpack_KVs(vbuf, vlen, key, value);
      if (found) {
        free(vbuf);
        return Status();
      }
      else 
        free(vbuf);
    }
  }                     
  else if (options.hint_packed == 1) { // large value                     
    kvssd::Slice get_key(key.data(), key.size());
    char *vbuf;
    int vlen;
    int ret = kvd_->kv_get(&get_key, vbuf, vlen);
    if (ret == 0) {
      value->append(vbuf, vlen);
      free(vbuf);
      return Status();
    }
    
  }

  // auto (fall back)
  // NOT IMPLEMENT
  

  return Status().NotFound(Slice());
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  return NewDBIterator(this, options);
}

Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr) {

  *dbptr = NULL;

  DB *db = new DBImpl(options, dbname);
  *dbptr = db;
  return Status(Status::OK());
}

}  // namespace kvrangedb
