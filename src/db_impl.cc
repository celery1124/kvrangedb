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
: options_(options) {
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
}

DBImpl::~DBImpl() {
  if (options_.cleanIndex) {
    for (int i = 0; i < options_.indexNum; i++) {
      std::string meta_name = std::to_string(i)+"/CURRENT";
      kvssd::Slice del_key_lsm(meta_name);
      kvd_->kv_delete(&del_key_lsm);
    }
  }
  for (int i = 0; i < options_.indexNum; i++)
    delete key_idx_[i];
	delete kvd_;
  printf("hitCnt = %d\n", hitCnt);
  printf("hitCost = %.3f, missCost = %.3f\n", hitCost, missCost);
  printf("hitNextCost = %.3f, missNextCost = %.3f\n", hitNextCost, missNextCost);
}

Status DBImpl::Put(const WriteOptions& options,
                     const Slice& key,
                     const Slice& value) {
  kvssd::Slice put_key(key.data(), key.size());
  kvssd::Slice put_val(value.data(), value.size());
  Monitor mon;
  kvd_->kv_store_async(&put_key, &put_val, on_io_complete, &mon);
  // index write
  int idx_id = (options_.indexNum == 0) ? 0 : MurmurHash64A(key.data(), key.size(), 0)%options_.indexNum;
  key_idx_[idx_id]->Put(key);
  
  mon.wait(); // wait data I/O done
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
  kvssd::Slice get_key(key.data(), key.size());
  char *vbuf;
	int vlen;
	kvd_->kv_get(&get_key, vbuf, vlen);
	value->append(vbuf, vlen);
  free(vbuf);

  return Status();
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
