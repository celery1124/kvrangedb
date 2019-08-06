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
  KVIndex* GetKVIndex() {return key_idx_;}

private:
  kvssd::KVSSD *kvd_;
  KVIndex *key_idx_;

  const Options options_;
};


}  // namespace kvrangedb



#endif