/******* kvrangedb *******/
/* db_impl.cc
* 07/23/2019
* by Mian Qin
*/
#include <mutex>
#include <atomic>
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
std::atomic<uint32_t> bloomHitCnt{0};
std::atomic<uint32_t> bloomFPCnt{0};
std::atomic<uint32_t> bloomMissCnt{0};
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
  // load meta
  load_meta(sequence_);
  pack_threads_ = new std::thread*[pack_threads_num];
  thread_m_ = new std::mutex[pack_threads_num];
  shutdown_ = new bool[pack_threads_num];
  for (int i = 0; i < pack_threads_num; i++) {
    shutdown_[i] = false;
    pack_threads_[i] = new std::thread(&DBImpl::processQ, this, i);
    printf("Initiated worker thread %d\n", i);
  }
  printf("Max Pack Size: %d\n", options_.packSize);
}

DBImpl::~DBImpl() {
  if (options_.cleanIndex) {
    for (int i = 0; i < options_.indexNum; i++) {
      std::string meta_name = std::to_string(i)+"/CURRENT";
      kvssd::Slice del_key_lsm(meta_name);
      kvd_->kv_delete(&del_key_lsm);
      printf("Clean index\n");
    }
  }
  sleep(1);

  if (options_.manualCompaction) {
    ManualCompaction();
    BuildFilter();
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

  // save meta (sequence number)
  save_meta();

  for (int i = 0; i < options_.indexNum; i++)
    delete key_idx_[i];
	delete kvd_;
  printf("hitCnt = %d\n", hitCnt);
  printf("hitCost = %.3f, missCost = %.3f\n", hitCost, missCost);
  printf("hitNextCost = %.3f, missNextCost = %.3f\n", hitNextCost, missNextCost);
  printf("bloomHitCnt = %d, bloomFPCnt = %d, bloomMissCnt = %d\n", bloomHitCnt.load(), bloomFPCnt.load(), bloomMissCnt.load());
  printf("bloomHitRatio = %.3f\n", (double)(bloomHitCnt.load()-bloomFPCnt.load())/(bloomHitCnt.load()+bloomMissCnt.load()));
  printf("bloomFPRatio = %.3f\n", (double)bloomFPCnt.load()/(bloomHitCnt.load()+bloomMissCnt.load()));
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
      Monitor mon;
      kvd_->kv_store_async(&pack_key, &pack_val, on_io_complete, &mon);
      
      // index write
      key_idx_[0]->Write(index_batch); // pack only support single index tree

      mon.wait(); // wait data I/O done
      
      // clean up
      free((char*) pack_key.data());
      free((char*) pack_val.data());
      delete index_batch;

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
    int idx_id = (options_.indexNum == 1) ? 0 : MurmurHash64A(key.data(), key.size(), 0)%options_.indexNum;
    key_idx_[idx_id]->Put(key);
    
    mon.wait(); // wait data I/O done
  }
  return Status();
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  kvssd::Slice del_key(key.data(), key.size());
	kvd_->kv_delete(&del_key);
  int idx_id = (options_.indexNum == 1) ? 0 : MurmurHash64A(key.data(), key.size(), 0)%options_.indexNum;
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
  bool possible_packed = true;
  bool possible_unpacked = true;
  if (options_.manualCompaction) { // capture hot query
    std::unique_lock<std::mutex> lock(seq_mutex_);
    hot_keys_[std::string(key.data(), key.size())]++;
  }
  if (options.hint_packed == 2) { // small value
    // index lookup
    Slice lkey(key.data(), key.size());
    std::string pkey;
    bool idx_found = key_idx_[0]->Get(lkey, pkey);
    if (!idx_found) { // definitely not found
      return Status().NotFound(Slice());
    }
    else if (pkey.size() == 0) {
      possible_packed = false;
      goto fallover;
    }
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
      else {
        free(vbuf);
        possible_packed = false;
      }
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
    else {
      free(vbuf);
      possible_unpacked = false;
    }
  }

  // auto (fall over)
fallover:
  // bloom filter check 
  bool filter_found = do_check_filter(key);
  if(possible_unpacked && filter_found) { // filter hit
    bloomHitCnt.fetch_add(1, std::memory_order_relaxed);
    kvssd::Slice get_key(key.data(), key.size());
    char *vbuf;
    int vlen;
    int ret = kvd_->kv_get(&get_key, vbuf, vlen);
    
    if (ret == 0) {
      value->append(vbuf, vlen);
      free(vbuf);
      return Status();
    }
    else {
      free(vbuf);
    }
  }

  if (possible_packed) { // filter miss
    bloomMissCnt.fetch_add(1, std::memory_order_relaxed);
    // index lookup
    Slice lkey(key.data(), key.size());
    std::string pkey;
    bool idx_found = key_idx_[0]->Get(lkey, pkey);
    if (!idx_found) { // definitely not found
      return Status().NotFound(Slice());
    }
    if(pkey.size() == 0) { // filter false positive
      bloomFPCnt.fetch_add(1, std::memory_order_relaxed);
      kvssd::Slice get_key(key.data(), key.size());
      char *vbuf;
      int vlen;
      int ret = kvd_->kv_get(&get_key, vbuf, vlen);
      if (ret == 0) {
        value->append(vbuf, vlen);
        free(vbuf);
        return Status();
      }
      else {
        free(vbuf);
        return Status().NotFound(Slice());
      }
    }
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
      else {
        free(vbuf);
        possible_packed = false;
      }
    }
  }
  
  return Status().NotFound(Slice());
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

void DBImpl::ManualCompaction() {
  options_.packDequeueTimeout = 60000; // increase dequeue timeout
  int pack_cnt = 0;
  int key_cnt = 0, last_key_cnt = 0;
  const int batch_size = 256;
  const ReadOptions options;
  IDXIterator *it_ = key_idx_[0]->NewIterator(options);
  it_->SeekToFirst();
  while(it_->Valid()) {

    // batch compact
    std::vector<std::string> key_batch;
    std::vector<kvssd::Slice> fetch_key_list;
    for (int i = 0; i < batch_size&&it_->Valid(); i++, it_->Next(), key_cnt++) {
      Slice lkey = it_->key();
      Slice pkey = it_->pkey();
      if (pkey.size() == 0 && hot_keys_.find(std::string(lkey.data(), lkey.size())) == hot_keys_.end()) {
        key_batch.push_back(std::string(lkey.data(), lkey.size()));
        fetch_key_list.push_back(key_batch[key_batch.size()-1]);
      }
    }
    assert(fetch_key_list.size() == key_batch.size());

    Monitor mon;
    std::mutex m;
    int fetch_num = key_batch.size();
    char **vbuf_list = new char*[fetch_num];
    uint32_t *actual_vlen_list = new uint32_t[fetch_num];
    Compaction_context *ctx = new Compaction_context {&m, 0, fetch_num, &mon};
    for (int i = 0; i < fetch_num; i++) {
      kvssd::Async_get_context *io_ctx = new kvssd::Async_get_context (vbuf_list[i], actual_vlen_list[i], (void *)ctx);
      kvd_->kv_get_async(&fetch_key_list[i], on_compact_get_complete, (void*) io_ctx);
    }

    mon.wait();

    Monitor mon_d;
    std::mutex m_d;
    Compaction_context *ctx_d = new Compaction_context {&m_d, 0, fetch_num, &mon_d};
    for (int i = 0; i < fetch_num; i++) {
      Slice lkey (key_batch[i]);
      Slice cold_val (vbuf_list[i], actual_vlen_list[i]);
      int size = get_phyKV_size(lkey, cold_val);
      packKVEntry *item = new packKVEntry(size, lkey, cold_val);
      while (pack_q_.size_approx() > options_.packQueueDepth) {
        pack_q_wait_.reset();
        pack_q_wait_.wait();
      }
      pack_q_.enqueue(item);
      pack_cnt++;
      kvd_->kv_delete_async(&fetch_key_list[i], on_compact_del_complete, (void*) ctx_d);
      free(vbuf_list[i]);
    }

    mon_d.wait();

    // clean up
    delete [] vbuf_list;
    delete [] actual_vlen_list;
    delete ctx;
    
    if (key_cnt - last_key_cnt >= 1000000) {
      printf("[ManualCompaction] total KVs %d, packed KVs %d\n", key_cnt, pack_cnt);
      last_key_cnt = key_cnt;
    }
  }
  printf("[ManualCompaction] total KVs %d, packed KVs %d\n", key_cnt, pack_cnt);
  delete it_;
}

void DBImpl::BuildFilter() {
  bf_.clear();
  BloomFilter bf(options_.filterBitsPerKey);
  int key_cnt = hot_keys_.size();
  std::vector<Slice> tmp_keys;
  for (auto it = hot_keys_.begin(); it != hot_keys_.end(); ++it) {
    tmp_keys.push_back(Slice(it->first));
  }
  bf.CreateFilter(&tmp_keys[0], key_cnt, &bf_);
  printf("Bloom Filter created, %d keys, %d bytes\n", key_cnt, bf_.size());
}

Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr) {

  *dbptr = NULL;

  DB *db = new DBImpl(options, dbname);
  *dbptr = db;
  return Status(Status::OK());
}

}  // namespace kvrangedb
