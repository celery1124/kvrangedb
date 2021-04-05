/******* kvrangedb *******/
/* kv_index_rocks.cc
* 09/08/2020
* by Mian Qin
*/

#include "kv_index.h"
#include "kvrangedb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/table.h"
#include "rocksdb/iterator.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/comparator.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/convenience.h"

namespace kvrangedb {


class ComparatorRocks : public rocksdb::Comparator {
public:
  ComparatorRocks(const kvrangedb::Comparator* cmp) : cmp_(cmp) {};
  //~ComparatorRocks() {};
  int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const {
    Slice aa(a.data(), a.size());
    Slice bb(b.data(), b.size());
    return cmp_->Compare(aa, bb);
  }
  const char* Name() const { return "Rocks.comparator"; }
  void FindShortestSeparator(
      std::string* start,
      const rocksdb::Slice& limit) const {
    // from rocksdb bytewise comparator
    // Find length of common prefix
    size_t min_length = std::min(start->size(), limit.size());
    size_t diff_index = 0;
    while ((diff_index < min_length) &&
          ((*start)[diff_index] == limit[diff_index])) {
      diff_index++;
    }

    if (diff_index >= min_length) {
      // Do not shorten if one string is a prefix of the other
    } else {
      uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);
      if (diff_byte < static_cast<uint8_t>(0xff) &&
          diff_byte + 1 < static_cast<uint8_t>(limit[diff_index])) {
        (*start)[diff_index]++;
        start->resize(diff_index + 1);
      }
    }
  }
  void FindShortSuccessor(std::string* key) const {
    // Find first character that can be incremented
    size_t n = key->size();
    for (size_t i = 0; i < n; i++) {
      const uint8_t byte = (*key)[i];
      if (byte != static_cast<uint8_t>(0xff)) {
        (*key)[i] = byte + 1;
        key->resize(i+1);
        return;
      }
    }
    // *key is a run of 0xffs.  Leave it alone.
  }
private:
  const kvrangedb::Comparator* cmp_;
};

class IDXWriteBatchRocks : public IDXWriteBatch {
public: 
  IDXWriteBatchRocks () : size_(0) {};
  ~IDXWriteBatchRocks () {};
  void Put(const Slice& key) {
    rocksdb::Slice put_key(key.data(), key.size());
    rocksdb::Slice put_val; //don't care;
    batch_.Put(put_key, put_val);
    size_++;
  }
  void Put(const Slice& lkey, const Slice& pkey) {
    rocksdb::Slice put_key(lkey.data(), lkey.size());
    rocksdb::Slice put_val(pkey.data(), pkey.size()); //don't care;
    batch_.Put(put_key, put_val);
    size_++;
  }
  void Delete(const Slice& key) {
    rocksdb::Slice put_key(key.data(), key.size());
    batch_.Delete(put_key);
    size_--;
  }
  void Clear() {
    size_ = 0;
    batch_.Clear();
  }
  void *InternalBatch() {return &batch_;}
  int Size() {return size_;}

public:
  rocksdb::WriteBatch batch_;
  int size_;
};

class IDXIteratorRocks : public IDXIterator {
public:
  IDXIteratorRocks(rocksdb::DB *db_, const ReadOptions& options) {
    rocksdb::ReadOptions rd_option;
    it_ = db_->NewIterator(rd_option);
  }
  ~IDXIteratorRocks() { delete it_; }

  bool Valid() const {it_->Valid();}
  void SeekToFirst() {it_->SeekToFirst();}
  void SeekToLast() {it_->SeekToLast();}
  void Seek(const Slice& target) {
    rocksdb::Slice seek_key(target.data(), target.size());
    it_->Seek(seek_key);
  }
  void Next() {it_->Next();}
  void Prev() {it_->Prev();}
  Slice key() const {
    rocksdb::Slice ret_key = it_->key();
    return Slice(ret_key.data(), ret_key.size());
  }
  Slice pkey() const {
    rocksdb::Slice ret_val = it_->value();
    return Slice(ret_val.data(), ret_val.size()); /* NOT IMPLEMENT */
  }
private:
  rocksdb::Iterator *it_;
};

class KVIndexRocks : public KVIndex {
public:
  KVIndexRocks (const Options& options, kvssd::KVSSD* kvd, std::string& name);
  ~KVIndexRocks ();

  // implmentations
  bool Put(const Slice& key);
  bool Put(const Slice& skey, const Slice& pkey);
  bool Get(const Slice& skey, std::string& pkey);
  bool Delete(const Slice& key);
  bool Write(IDXWriteBatch* updates);
  IDXIterator* NewIterator(const ReadOptions& options);
 
private:
  std::string name_;
  rocksdb::DB* db_;
  rocksdb::WriteOptions write_options_;
  rocksdb::ReadOptions read_options_;
  kvssd::KVSSD* kvd_;
  ComparatorRocks* cmp_;
};

KVIndexRocks::KVIndexRocks(const Options& db_options, kvssd::KVSSD* kvd, std::string& name) : name_(name), kvd_(kvd) {
  rocksdb::Options options;
  options.IncreaseParallelism();
  //options.OptimizeLevelStyleCompaction();
  options.create_if_missing = true;
  options.max_open_files = 1000;
  options.compression = rocksdb::kNoCompression;
  options.paranoid_checks = false;
  options.write_buffer_size = 64 << 20;
  options.target_file_size_base = 64 * 1048576;
  options.max_bytes_for_level_base = 64 * 1048576;

  rocksdb::BlockBasedTableOptions table_options;
  // table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(14, false));
  table_options.block_size = 16384;
  //table_options.cache_index_and_filter_blocks = true;
  if (db_options.indexCacheSize > 0)
    table_options.block_cache = rocksdb::NewLRUCache((size_t)db_options.indexCacheSize * 1024 * 1024LL);
  else {
    //table_options.block_cache = rocksdb::NewLRUCache(16384);
    table_options.no_block_cache = true; 
  }
  options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

  // apply db options
  cmp_ = new ComparatorRocks(db_options.comparator);
  options.comparator = cmp_;

  options.env = rocksdb::NewKVEnvOpt(rocksdb::Env::Default(), kvd);
  rocksdb::Status status = rocksdb::DB::Open(options, name, &db_);
  if (status.ok()) printf("rocksdb open ok\n");
  else printf("rocksdb open error\n");

  write_options_.disableWAL = true;
}

KVIndexRocks::~KVIndexRocks() {
  rocksdb::CancelAllBackgroundWork(db_, true);
  delete db_;
  delete cmp_;
}

KVIndex* NewRocksIndex(const Options& options, kvssd::KVSSD* kvd, std::string& name) {
  return new KVIndexRocks(options, kvd, name);
}

IDXWriteBatch* NewIDXWriteBatchRocks() {
  return new IDXWriteBatchRocks();
}

bool KVIndexRocks::Put(const Slice &key) {
  rocksdb::Slice put_key(key.data(), key.size());
  rocksdb::Slice put_val; // don't care
  return (db_->Put(write_options_, put_key, put_val)).ok();
}

bool KVIndexRocks::Put(const Slice& skey, const Slice& pkey) {
  rocksdb::Slice put_key(skey.data(), skey.size());
  rocksdb::Slice put_val(pkey.data(), pkey.size()); 
  return (db_->Put(write_options_, put_key, put_val)).ok();
}

bool KVIndexRocks::Get(const Slice& skey, std::string& pkey) { 
  rocksdb::Slice get_key(skey.data(), skey.size());
  return (db_->Get(read_options_, get_key, &pkey)).ok();
}

bool KVIndexRocks::Delete(const Slice &key) {
  rocksdb::Slice put_key(key.data(), key.size());
  return (db_->Delete(write_options_, put_key)).ok();
}

bool KVIndexRocks::Write(IDXWriteBatch* updates) {
  return (db_->Write(write_options_, (rocksdb::WriteBatch*)(updates->InternalBatch()))).ok();
}

IDXIterator* KVIndexRocks::NewIterator(const ReadOptions& options) {
  return new IDXIteratorRocks(db_, options);
}

}  // namespace kvrangedb
