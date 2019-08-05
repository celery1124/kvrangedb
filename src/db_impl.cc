/******* kvrangedb *******/
/* db.cc
* 07/23/2019
* by Mian Qin
*/
#include <mutex>
#include <condition_variable>
#include "kvrangedb/db.h"
#include "kvrangedb/iterator.h"
#include "db_impl.h"

namespace kvrangedb {

class DBIterator : public Iterator {
public:
  DBIterator(DBImpl *db, const ReadOptions &options) : kvd_(db->GetKVSSD()) {
    it_ = db->GetKVIndex()->NewIterator(options);
  }
  ~DBIterator() { delete it_; }

  bool Valid() const {
    return it_->Valid();
  }
  void SeekToFirst() { it_->SeekToFirst(); }
  void SeekToLast() { it_->SeekToLast(); }
  void Seek(const Slice& target) { it_->Seek(target); }
  void Next() { it_->Next(); }
  void Prev() { it_->Prev(); }
  Slice key() const { return it_->key(); }
  Slice value();
private:
  IDXIterator *it_;
  kvssd::KVSSD* kvd_;
  std::string value_;
};

Slice DBIterator::value() {
  Slice curr_key = key();
  kvssd::Slice get_key(curr_key.data(), curr_key.size());
  char *vbuf;
	int vlen;
	kvd_->kv_get(&get_key, vbuf, vlen);
  value_.clear();
	value_.append(vbuf, vlen);
  free(vbuf);

  return Slice(value_);
}

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

static void on_io_complete(void *args) {
    Monitor *mon = (Monitor *)args;
    mon->notify();
}

DBImpl::DBImpl(const Options& options, const std::string& dbname) {
  kvd_ = new kvssd::KVSSD(dbname.c_str());
  if (options.indexType == LSM)
	  key_idx_ = NewLSMIndex(options, kvd_);
  else if (options.indexType == BTREE)
    key_idx_ = NewBTreeIndex(options, kvd_);
  else {
    printf("WRONG KV INDEX TYPE\n");
    exit(-1);
  }
}

DBImpl::~DBImpl() {
  delete key_idx_;
	delete kvd_;
}

Status DBImpl::Put(const WriteOptions& options,
                     const Slice& key,
                     const Slice& value) {
  kvssd::Slice put_key(key.data(), key.size());
  kvssd::Slice put_val(value.data(), value.size());
  Monitor mon;
  kvd_->kv_store_async(&put_key, &put_val, on_io_complete, &mon);
  key_idx_->Put(key);

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
  return new DBIterator(this, options);
}

Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr) {

  *dbptr = NULL;

  DB *db = new DBImpl(options, dbname);
  *dbptr = db;
  return Status(Status::OK());
}

}  // namespace kvrangedb
