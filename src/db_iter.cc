/******* kvrangedb *******/
/* db_iter.cc
* 08/06/2019
* by Mian Qin
*/
#include <mutex>
#include <iostream>
#include <condition_variable>
#include <unordered_map>
#include "kvrangedb/iterator.h"
#include "db_impl.h"
#include "db_iter.h"
#include "kvssd/kvssd.h"
#include <thread>
#include <atomic>

#include <ctime>
#include <chrono>

// // meant to measure the benefit of Hot query acceleration
// int hitCnt = 0;
// double hitCost = 0;
// double missCost = 0;
// double hitNextCost = 0;
// double missNextCost = 0;
namespace kvrangedb {

std::unordered_map<std::string, int> scan_queryKey_;

class Prefetch_context{
public:
  std::atomic<int> prefetch_cnt;
  int prefetch_num;
  Monitor *mon;
  Prefetch_context (int prefetch_num_, Monitor *mon_) : prefetch_cnt(0), prefetch_num(prefetch_num_), mon(mon_) {}
} ;

void on_prefetch_complete(void* args) {
  Prefetch_context *prefetch_ctx = (Prefetch_context *)args;
  if (prefetch_ctx->prefetch_cnt.fetch_add(1) == prefetch_ctx->prefetch_num-1)
    prefetch_ctx->mon->notify();

}

class MergeIterator : public Iterator {
public:
  MergeIterator(DBImpl *db, const ReadOptions &options, int n) : 
        comparator_(db->GetComparator()),
        n_(n),
        children_(new IDXIterator*[n]),
        current_(NULL) {
      for (int i = 0; i < n; i++) {
        children_[i] = db->GetKVIndex(i)->NewIterator(options); 
      }
  };
  ~MergeIterator() {
    for (int i = 0; i < n_; i++) {
      delete children_[i];
    }
    delete [] children_; 
  };

  bool Valid() const {return (current_ != NULL) && current_->Valid();};
  void SeekToFirst() {
    if (n_ == 1) { // fast path
      children_[0]->SeekToFirst();
      current_ = children_[0];
    }
    else { // parallel seek
      std::thread **t = new std::thread*[n_];
      for (int i = 0 ; i < n_; i++) {
        t[i] = new std::thread (&IDXIterator::SeekToFirst, children_[i]);
      }
      for (int i = 0; i < n_; i++) {
        t[i]->join();
        delete t[i];
      }
      delete [] t;
      FindSmallest();
    }
  };
  void SeekToLast() { /* NOT IMPLEMENT */ }
  void Seek(const Slice& target) {
    // for (int i = 0; i < n_; i++) {
    //   children_[i]->Seek(target);
    // }
    // FindSmallest();
    if (n_ == 1) { // fast path
      children_[0]->Seek(target);
      current_ = children_[0];
    }
    else { // parallel seek
      std::thread **t = new std::thread*[n_];
      for (int i = 0 ; i < n_; i++) {
        t[i] = new std::thread (&IDXIterator::Seek, children_[i], target);
      }
      for (int i = 0; i < n_; i++) {
        t[i]->join();
        delete t[i];
      }
      delete [] t;
      FindSmallest();
    }
  };
  void Next() {
    assert(Valid());

    current_->Next();
    FindSmallest();
  };
  void Prev() { /* NOT FULLY IMPLEMENT, ONLY SUPPORT SINGLE INDEX */ 
    assert(Valid());

    current_->Prev();
  }
  Slice key() const {
    assert(Valid());
    return current_->key();
  };
  Slice value() { 
    assert(Valid());
    return current_->pkey();
  };

private:
  const Comparator* comparator_;
  void FindSmallest() {
    IDXIterator* smallest = NULL;
    for (int i = 0; i < n_; i++) {
      IDXIterator* child = children_[i];
      if (child->Valid()) {
        if (smallest == NULL) {
          smallest = child;
        } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
          smallest = child;
        }
      }
    }
    current_ = smallest;
  };

  int n_;
  IDXIterator** children_;
  IDXIterator* current_;

};

class DBIterator : public Iterator {
public:
  DBIterator(DBImpl *db, const ReadOptions &options);
  ~DBIterator();

  bool Valid() const {
    return valid_;
  }
  void SeekToFirst();
  void SeekToLast() { /* NOT IMPLEMENT */ }
  void Seek(const Slice& target);
  void Next();
  void Prev();
  Slice key() const;
  Slice value();
private:
  DBImpl *db_;
  const ReadOptions &options_;
  MergeIterator *it_;
  kvssd::KVSSD *kvd_;
  std::string value_;
  bool valid_;

  // upper key hint
  Slice upper_key_;

  // for value prefetch
  bool prefetch_ena_;
  std::string *key_queue_;
  std::string *pkey_queue_;
  Slice *val_queue_;
  bool *valid_queue_;
  int prefetch_depth_;
  int queue_cur_;
  std::unordered_map<std::string, Slice> readahead_cache_; // small cache for packed value

  // pack key range
  bool isPacked_;
  std::string packedKVs_;
  std::string helper_key_;
  int packedIdx_;
  Slice isPackedCurrKey_;
  Slice isPackedCurrVal_;

  void prefetch_value(std::vector<Slice>& key_list, std::vector<Slice>& lkey_list, std::vector<Slice>& val_list);
  void prefetch_value_readahead(std::vector<Slice>& key_list, std::vector<Slice>& lkey_list, std::vector<Slice>& val_list);
};

DBIterator::DBIterator(DBImpl *db, const ReadOptions &options) 
: db_(db), options_(options), kvd_(db->GetKVSSD()), valid_(false),
  queue_cur_(0), isPacked_(false), packedIdx_(0) {
  
  prefetch_depth_ = (options.scan_length + 1 >= db_->options_.prefetchDepth) ? db->options_.prefetchDepth : options.scan_length + 1;
  // whether to prefetch?
  prefetch_ena_ = db_->options_.prefetchEnabled && 
  (db_->inflight_io_count_.load(std::memory_order_relaxed) < db_->options_.prefetchReqThres);

  if (options_.upper_key != NULL) {
    upper_key_ = *(options_.upper_key);
  }
  if (prefetch_ena_) {
    int prefetch_depth = db_->options_.prefetchDepth;
    key_queue_ = new std::string[prefetch_depth];
    pkey_queue_ = new std::string[prefetch_depth];
    val_queue_ = new Slice[prefetch_depth];
    valid_queue_ = new bool[prefetch_depth];
    for (int i = 0 ; i < prefetch_depth; i++) {
      valid_queue_[i] = false;
      val_queue_[i].clear();
    }
  }
  //printf("prefetch_depth_ = %d\n", prefetch_depth_);
  it_ = new MergeIterator(db, options, db->options_.indexNum);
}

DBIterator::~DBIterator() { 
  // write packed key range
  if (helper_key_.size() > 0 ) {
    std::string helper_key = helper_key_+"helper";
    kvssd::Slice put_key(helper_key.data(), helper_key.size());
    kvssd::Slice put_val(packedKVs_.data(), packedKVs_.size());

    kvd_->kv_store(&put_key, &put_val);
    scan_queryKey_[helper_key_] = -1;
    printf("write packed key range, start key: %s, total size: %d\n", helper_key.c_str(), packedKVs_.size());
  }
  delete it_; 

  if (prefetch_ena_) {
    delete [] key_queue_;
    delete [] pkey_queue_;
    for (int i = 0 ; i < db_->options_.prefetchDepth; i++) {
      if (val_queue_[i].size()) {
        free((void *)val_queue_[i].data());
      }
    }
    delete [] val_queue_;
    delete [] valid_queue_;
  }
}

static bool do_unpack_KVs (char *vbuf, int size, const Slice& lkey, char*& lvalue, int& lvsize, std::unordered_map<std::string, Slice>& cache) {
  char *p = vbuf;
  while ((p-vbuf) < size) {
    uint8_t key_len = *((uint8_t*)p);
    p += sizeof(uint8_t);
    Slice extract_key (p, key_len);
    p += key_len;
    uint32_t val_len = *((uint32_t*)p);
    p += sizeof(uint32_t);
    if (lkey.compare(extract_key) == 0) {
      lvsize = val_len;
      lvalue = (char *)malloc(val_len);
      memcpy(lvalue, p, val_len);
    } else {
      char *val = (char *)malloc(val_len);
      memcpy(val, p, val_len);
      cache[extract_key.ToString()] = Slice(val, val_len);
    }
    
    p += val_len;
    assert ((p-vbuf) < size);
  }
  return true;
}

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
    assert ((p-vbuf) < size);
  }
  return false;
}

static bool do_unpack_KVs (char *vbuf, int size, const Slice& lkey, char*& lvalue, int& lvsize) {
  char *p = vbuf;
  while ((p-vbuf) < size) {
    uint8_t key_len = *((uint8_t*)p);
    p += sizeof(uint8_t);
    Slice extract_key (p, key_len);
//printf("%s, %s ",std::string(lkey.data(), lkey.size()).c_str(), std::string(p, key_len).c_str());
    p += key_len;
    uint32_t val_len = *((uint32_t*)p);
//printf("vlen %d, total size %d\n", val_len, size);
    p += sizeof(uint32_t);
    if (lkey.compare(extract_key) == 0) {
//printf("found\n");
      lvsize = val_len;
      lvalue = (char *)malloc(val_len);
      memcpy(lvalue, p, val_len);
      return true;
    } else {
      p += val_len;
    }
    if(p-vbuf >= size) {
      lvsize = 1000;
      lvalue = (char*)malloc(lvsize);
      return true;
    }
    assert ((p-vbuf) < size);
  }
  return false;
}

void DBIterator::prefetch_value(std::vector<Slice>& key_list, std::vector<Slice>& lkey_list, 
                std::vector<Slice>& val_list) {
  int prefetch_num = key_list.size();
  char **vbuf_list = new char*[prefetch_num];
  uint32_t *actual_vlen_list = new uint32_t[prefetch_num];
  Monitor mon;
  Prefetch_context *ctx = new Prefetch_context (prefetch_num, &mon);

  std::vector<kvssd::Slice> prefetch_key_list;
  db_->inflight_io_count_.fetch_add(prefetch_num, std::memory_order_relaxed);
  for (int i = 0 ; i < prefetch_num; i++) {
    prefetch_key_list.push_back(kvssd::Slice(key_list[i].data(), key_list[i].size()));
    //kvssd::Async_get_context *io_ctx = new kvssd::Async_get_context {vbuf_list[i], actual_vlen_list[i], (void *)ctx};
    kvssd::Async_get_context *io_ctx = new kvssd::Async_get_context (kvd_, vbuf_list[i], actual_vlen_list[i], (void *)ctx);
    kvd_->kv_get_async(&prefetch_key_list[i], on_prefetch_complete, (void*) io_ctx);
  }

  mon.wait();
  db_->inflight_io_count_.fetch_sub(prefetch_num, std::memory_order_relaxed);
  // save the vbuf
  for (int i = 0; i < prefetch_num; i++) {
    if (lkey_list[i].size() == 0)
      val_list.push_back(Slice(vbuf_list[i], actual_vlen_list[i]));
    else { // unpack KV
      char *lvalue = nullptr;
      int lvsize;
      Slice lkey(lkey_list[i]);
      do_unpack_KVs(vbuf_list[i], actual_vlen_list[i], lkey, lvalue, lvsize);
      val_list.push_back(Slice(lvalue, lvsize));
      free(vbuf_list[i]);
    }
  }
  
  // de-allocate resources
  delete [] vbuf_list;
  delete [] actual_vlen_list;
  delete ctx;
}

void DBIterator::prefetch_value_readahead(std::vector<Slice>& key_list, std::vector<Slice>& lkey_list, 
                std::vector<Slice>& val_list) {
  int prefetch_num = key_list.size();
  int async_get_num = 0;
  for (int i = 0; i < prefetch_num; i++) {
    val_list.push_back(Slice());
    if (lkey_list[i].size() == 0)
      async_get_num++;
  }
  char **vbuf_list = new char*[prefetch_num];
  uint32_t *actual_vlen_list = new uint32_t[prefetch_num];
  Monitor mon;
  Prefetch_context *ctx = new Prefetch_context (async_get_num, &mon);
  kvssd::Slice *async_get_key_list = new kvssd::Slice[prefetch_num];

  if (async_get_num > 0) {
    db_->inflight_io_count_.fetch_add(async_get_num, std::memory_order_relaxed);
    for (int i = 0; i < prefetch_num; i++) {
      if (lkey_list[i].size() == 0) { // hot key, no translation
        async_get_key_list[i] = kvssd::Slice (key_list[i].data(), key_list[i].size());
        kvssd::Async_get_context *io_ctx = new kvssd::Async_get_context (kvd_, vbuf_list[i], actual_vlen_list[i], (void *)ctx);
        kvd_->kv_get_async(&async_get_key_list[i], on_prefetch_complete, (void*) io_ctx);  
      }
    }
  }

  for (int i = 0; i < prefetch_num; i++) {
    if (lkey_list[i].size() > 0) { // packed key
      std::string lkey = lkey_list[i].ToString();
      if (readahead_cache_.find(lkey) == readahead_cache_.end()) { // need I/O
        char *vbuf = NULL;
        int actual_vlen;
        
        db_->inflight_io_count_.fetch_add(1, std::memory_order_relaxed); 
        kvssd::Slice prefetch_key (key_list[i].data(), key_list[i].size());
        kvd_->kv_get(&prefetch_key, vbuf, actual_vlen, 64<<10);
        db_->inflight_io_count_.fetch_sub(1, std::memory_order_relaxed);
        // push each logical record into readahead cache
        Slice lkey_s(lkey_list[i]);
        char *lvalue = nullptr;
        int lvsize;
        do_unpack_KVs(vbuf, actual_vlen, lkey_s, lvalue, lvsize, readahead_cache_);
        val_list[i] = Slice(lvalue, lvsize);
        free(vbuf);
      }
      else { // already in cache (packed record read in single shot)
        val_list[i] = readahead_cache_[lkey];
      }
    } 
  }

  if (async_get_num > 0) {
    mon.wait();
    db_->inflight_io_count_.fetch_sub(async_get_num, std::memory_order_relaxed);
  
    for (int i = 0; i < prefetch_num; i++) {
      if (lkey_list[i].size() == 0)
        val_list[i] = Slice(vbuf_list[i], actual_vlen_list[i]);
    }
  }

  // de-allocate resources
  delete [] vbuf_list;
  delete [] actual_vlen_list;
  delete ctx;
  delete [] async_get_key_list;
}

void DBIterator::SeekToFirst() { 
  it_->SeekToFirst();
  if (prefetch_ena_) {
    valid_ = valid_queue_[0] = it_->Valid();
    if (it_->Valid())
      key_queue_[0] = it_->key().ToString();
      pkey_queue_[0] = it_->value().ToString();
  }
  else {
    valid_ = it_->Valid();
  }
}

static bool isHelperKV(char *vbuf, int vlen) {
  assert(vlen >= sizeof(int));
  int *header = (int*)vbuf;
  return (*header) == 0xA5A5A5A5;
}

void DBIterator::Seek(const Slice& target) { 
  RecordTick(db_->options_.statistics.get(), REQ_SEEK);
  // check range filter if needed
  if (upper_key_.size() > 0 && db_->rf_ && !db_->rf_->RangeMayMatch(target, upper_key_)) {
    RecordTick(db_->options_.statistics.get(), FILTER_RANGE_NEGATIVE);
    valid_ = false;
    return;
  }
  RecordTick(db_->options_.statistics.get(), FILTER_RANGE_POSITIVE);
  
  auto wcts = std::chrono::system_clock::now();
  // training helper record
  if (db_->options_.helperHint == 1) {
    std::string queryKey (target.data(), target.size());
    if (scan_queryKey_[queryKey] >= 0) scan_queryKey_[queryKey]++;
    if (scan_queryKey_[queryKey] >= db_->options_.helperTrainingThres) {
      helper_key_ = queryKey;
      int header = 0xA5A5A5A5;
      packedKVs_.append((char*)&header, sizeof(int));
    }
  }
  else if (db_->options_.helperHint == 2) { // infer phases
    // get helper I/O
    std::string helper_key = std::string(target.data(), target.size()) + "helper";
    kvssd::Slice probe_key(helper_key);
    char *vbuf;
    int vlen;
    kvs_result ret;
    ret = kvd_->kv_get(&probe_key, vbuf, vlen, 256<<10);
    bool helperHit = (ret!=KVS_ERR_KEY_NOT_EXIST) && isHelperKV(vbuf, vlen);
    if (helperHit) { // fast path
      packedKVs_.append(vbuf+sizeof(int), vlen-sizeof(int));
      isPacked_ = true;
      valid_ = true;
      // hitCnt++;
      free(vbuf);
      // std::chrono::duration<double, std::micro> hitduration = (std::chrono::system_clock::now() - wcts);
      // hitCost += hitduration.count();
      return;
    }
  }
  // none phase
  it_->Seek(target); 
  if (prefetch_ena_) {
    if (upper_key_.size() > 0) { // upper key specified
      valid_ = valid_queue_[0] = (it_->Valid() && db_->options_.comparator->Compare(it_->key(), upper_key_) < 0);
    }
    else
      valid_ = valid_queue_[0] = it_->Valid();
    if (valid_) {
      key_queue_[0] = it_->key().ToString();
      pkey_queue_[0] = it_->value().ToString();
    }
    // implicit next for prefetch
    assert(queue_cur_ == 0);
    if (upper_key_.size() > 0) { // upper key specified
      for (int i = 1; i < prefetch_depth_; i++) {
        if (it_->Valid() && db_->options_.comparator->Compare(it_->key(), upper_key_) < 0) {
          it_->Next();
          if(it_->Valid()) {
            key_queue_[i] = (it_->key()).ToString();
            pkey_queue_[i] = (it_->value()).ToString();
            valid_queue_[i] = true;
          }
          else {
            valid_queue_[i] = false;
            break;
          }
        }
      }
    }
    else { // upper key not specified 
      for (int i = 1; i < prefetch_depth_; i++) {
        if (it_->Valid()) {
          it_->Next();
          if(it_->Valid()) {
            key_queue_[i] = (it_->key()).ToString();
            pkey_queue_[i] = (it_->value()).ToString();
            valid_queue_[i] = true;
          }
          else {
            valid_queue_[i] = false;
            break;
          }
        }
      }
    }
    
  }
  else {
    if (upper_key_.size() > 0) { // upper key specified
      valid_ = (it_->Valid() && db_->options_.comparator->Compare(it_->key(), upper_key_) < 0);
    }
    else {
      valid_ = it_->Valid();
    }
  }
  
  // std::chrono::duration<double, std::micro> missduration = (std::chrono::system_clock::now() - wcts);
  // missCost += missduration.count();
}

void DBIterator::Prev() { /* NOT FULLY IMPLEMENT, Suppose ONLY CALL BEFORE next */ 
  assert(valid_);
  std::string curr_key = it_->key().ToString();

  do {
    it_->Prev();
  } while (it_->Valid() && db_->options_.comparator->Compare(it_->key(), curr_key) >= 0);
  valid_ = it_->Valid();
  if (valid_ && prefetch_ena_) {
    key_queue_[0] = it_->key().ToString();
    pkey_queue_[0] = it_->value().ToString();
  }
}

void DBIterator::Next() {
  RecordTick(db_->options_.statistics.get(), REQ_NEXT);
  auto wcts = std::chrono::system_clock::now();
  assert(valid_);
  if (isPacked_) {
    auto wcts = std::chrono::system_clock::now();
    if (packedIdx_ >= packedKVs_.size()) {
      valid_ = false;
      return;
    }
    int klen;
    int vlen;
    memcpy(&klen, &packedKVs_[packedIdx_], sizeof(int));
    packedIdx_ += sizeof(int);
    isPackedCurrKey_ = Slice(&packedKVs_[packedIdx_], klen);
    packedIdx_ += klen;
    memcpy(&vlen, &packedKVs_[packedIdx_], sizeof(int));
    packedIdx_ += sizeof(int);
    isPackedCurrVal_ = Slice(&packedKVs_[packedIdx_], vlen);
    packedIdx_ += vlen;
    // std::chrono::duration<double, std::micro> duration = (std::chrono::system_clock::now() - wcts);
    // hitNextCost += duration.count();
  }
  else if (prefetch_ena_) {
    if (queue_cur_ == prefetch_depth_-1) {
      queue_cur_ = 0; //reset cursor
      // release allocated memory vbuf
      for (int i = 0; i < prefetch_depth_; i++) {
        if (val_queue_[i].size()) free ((void *)val_queue_[i].data());
        val_queue_[i].clear();
      }
      // calculate prefetch depth 
      if (prefetch_depth_ < db_->options_.prefetchDepth) {
        prefetch_depth_ = prefetch_depth_ == 0 ? 1 : prefetch_depth_ << 1;
      }

      if (upper_key_.size() > 0) { // upper key specified
        for (int i = 1; i < prefetch_depth_; i++) {
          if (it_->Valid() && db_->options_.comparator->Compare(it_->key(), upper_key_) < 0) {
            it_->Next();
            if(it_->Valid()) {
              key_queue_[i] = (it_->key()).ToString();
              pkey_queue_[i] = (it_->value()).ToString();
              valid_queue_[i] = true;
            }
            else {
              valid_queue_[i] = false;
              break;
            }
          }
        }
      }
      else { // upper key not specified 

        for (int i = 0; i < prefetch_depth_; i++) {
          it_->Next();
          valid_ = it_->Valid();
          if(valid_) {
            key_queue_[i] = (it_->key()).ToString();
            pkey_queue_[i] = (it_->value()).ToString();
            valid_queue_[i] = true;
          }
          else {
            valid_queue_[i] = false;
            break;
          }
        }
      }
    }
    else
      queue_cur_++;
    
    valid_ = valid_queue_[queue_cur_];
    // std::chrono::duration<double, std::micro> duration = (std::chrono::system_clock::now() - wcts);
    // missNextCost += duration.count();
  }
  else {
    it_->Next();
    // valid_ = it_->Valid();
    if (upper_key_.size() > 0) { // upper key specified
      valid_ = (it_->Valid() && db_->options_.comparator->Compare(it_->key(), upper_key_) < 0);
    }
    else {
      valid_ = it_->Valid();
    }
  }
  
}

Slice DBIterator::key() const {
  assert(valid_);
  if (isPacked_) {
    return isPackedCurrKey_;
  }
  else if (prefetch_ena_)
    return Slice(key_queue_[queue_cur_]);
  else
    return it_->key();
}

Slice DBIterator::value() {
  assert(valid_);
  
  if (isPacked_) {
    return isPackedCurrVal_;
  }
  else if (prefetch_ena_) {
    if (queue_cur_ == 0) {// do prefetch_value
      std::vector<Slice> key_list;
      std::vector<Slice> lkey_list;
      std::vector<Slice> val_list;

      for (int i = 0; i < prefetch_depth_; i++) {
        if(valid_queue_[i]) {
          if (pkey_queue_[i].size() > 0) {
            key_list.push_back(Slice(pkey_queue_[i]));
            lkey_list.push_back(key_queue_[i]);
          } else {
            key_list.push_back(Slice(key_queue_[i]));
            lkey_list.push_back(Slice());
          }
        }
        else break;
      }
      // prefetch_value(key_list, lkey_list, val_list);
      prefetch_value_readahead(key_list, lkey_list, val_list);
      for (int i = 0; i < val_list.size(); i++) {
        val_queue_[i] = val_list[i];
      }

    }
    if (helper_key_.size() > 0 ) {
      int klen = key_queue_[queue_cur_].size();
      int vlen = val_queue_[queue_cur_].size();
      packedKVs_.append((char*)&klen, sizeof(int));
      packedKVs_.append(key_queue_[queue_cur_]);
      packedKVs_.append((char*)&vlen, sizeof(int));
      packedKVs_.append(val_queue_[queue_cur_].ToString());
    }
    return val_queue_[queue_cur_];
  }
  else {
    Slice curr_key = key();
    if (it_->value().size()) { // packed KV
      Slice phy_key = it_->value();
      kvssd::Slice get_key(phy_key.data(), phy_key.size());
      char *vbuf;
      int vlen;
      kvd_->kv_get(&get_key, vbuf, vlen);
      value_.clear();
      bool found = do_unpack_KVs(vbuf, vlen, curr_key, &value_);
      assert(found);
      free(vbuf);
    } else {
      kvssd::Slice get_key(curr_key.data(), curr_key.size());
      char *vbuf;
      int vlen;
      kvd_->kv_get(&get_key, vbuf, vlen);
      value_.clear();

      value_.append(vbuf, vlen);
      free(vbuf);
    }

    if (helper_key_.size() > 0 ) {
      int klength = curr_key.size();
      int vlength = value_.size();
      packedKVs_.append((char*)&klength, sizeof(int));
      packedKVs_.append(curr_key.data(), curr_key.size());
      packedKVs_.append((char*)&vlength, sizeof(int));
      packedKVs_.append(value_.data(), value_.size());
    }
    return Slice(value_);
  }

}

Iterator* NewDBIterator(DBImpl *db, const ReadOptions &options) {
  return new DBIterator(db, options);
}

} // end namespace kvrangedb
