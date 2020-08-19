/******* kvrangedb *******/
/* db_impl.cc
* 07/23/2019
* by Mian Qin
*/
#include <mutex>
#include <condition_variable>
#include "kvrangedb/db.h"
#include "kvrangedb/iterator.h"
#include "db_impl.h"
#include "db_iter.h"

extern int hitCnt;
extern double hitCost;
extern double missCost;
extern double hitNextCost;
extern double missNextCost;
namespace kvrangedb {

static void on_io_complete(void *args) {
    Monitor *mon = (Monitor *)args;
    mon->notify();
}

DBImpl::DBImpl(const Options& options, const std::string& dbname) 
: options_(options) {
  kvd_ = new kvssd::KVSSD(dbname.c_str());
  if (options.indexType == LSM) {
	  key_idx_ = NewLSMIndex(options, kvd_);
    idx_batch_ = NewIDXWriteBatchLSM();
  }
  else if (options.indexType == LSMOPT) {
    key_idx_ = NewLSMIndex(options, kvd_);
    idx_batch_ = NewIDXWriteBatchLSM();
  }
  else if (options.indexType == BTREE) {
    key_idx_ = NewBTreeIndex(options, kvd_);
    idx_batch_ = NewIDXWriteBatchBTree();
  }
  else if (options.indexType == BASE) {
    key_idx_ = NewBaseIndex(options, kvd_);
    idx_batch_ = NewIDXWriteBatchBase();
  }
  else if (options.indexType == INMEM) {
    key_idx_ = NewInMemIndex(options, kvd_);
    idx_batch_ = NewIDXWriteBatchInmem();
  }
  else {
    printf("WRONG KV INDEX TYPE\n");
    exit(-1);
  }
  if (options.cleanIndex) {
    kvssd::Slice del_key_lsm("/CURRENT");
    kvd_->kv_delete(&del_key_lsm);
  }
}

DBImpl::~DBImpl() {
  if (idx_batch_->Size()) {
    key_idx_->Write(idx_batch_);
    idx_batch_->Clear();
  }
  delete key_idx_;
  delete idx_batch_;
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
  //key_idx_->Put(key);
  
  if (idx_batch_->Size() < 8) {
    idx_batch_->Put(key);
  }
  else {
    key_idx_->Write(idx_batch_);
    idx_batch_->Clear();
  }
  mon.wait(); // wait data I/O done
  return Status();
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  kvssd::Slice del_key(key.data(), key.size());
	kvd_->kv_delete(&del_key);
  key_idx_->Delete(key);
  return Status();
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {

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
