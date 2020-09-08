/******* kvrangedb *******/
/* kv_index.h
* 07/23/2019
* by Mian Qin
*/

#include "kv_index.h"
#include "kvrangedb/comparator.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/write_batch.h"
#include "leveldb/comparator.h"

namespace kvrangedb {


class ComparatorLSM : public leveldb::Comparator {
public:
  ComparatorLSM(const kvrangedb::Comparator* cmp) : cmp_(cmp) {};
  //~ComparatorLSM() {};
  int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const {
    Slice aa(a.data(), a.size());
    Slice bb(b.data(), b.size());
    return cmp_->Compare(aa, bb);
  }
  const char* Name() const { return "lsm.comparator"; }
  void FindShortestSeparator(
      std::string* start,
      const leveldb::Slice& limit) const {
    // from leveldb bytewise comparator
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

class IDXWriteBatchLSM : public IDXWriteBatch {
public: 
  IDXWriteBatchLSM () : size_(0) {};
  ~IDXWriteBatchLSM () {};
  void Put(const Slice& key) {
    leveldb::Slice put_key(key.data(), key.size());
    leveldb::Slice put_val; //don't care;
    batch_.Put(put_key, put_val);
    size_++;
  }
  void Put(const Slice& lkey, const Slice& pkey) {
    leveldb::Slice put_key(lkey.data(), lkey.size());
    leveldb::Slice put_val(pkey.data(), pkey.size()); //don't care;
    batch_.Put(put_key, put_val);
    size_++;
  }
  void Delete(const Slice& key) {
    leveldb::Slice put_key(key.data(), key.size());
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
  leveldb::WriteBatch batch_;
  int size_;
};

class IDXIteratorLSM : public IDXIterator {
public:
  IDXIteratorLSM(leveldb::DB *db_, const ReadOptions& options) {
    leveldb::ReadOptions rd_option;
    it_ = db_->NewIterator(rd_option);
  }
  ~IDXIteratorLSM() { delete it_; }

  bool Valid() const {it_->Valid();}
  void SeekToFirst() {it_->SeekToFirst();}
  void SeekToLast() {it_->SeekToLast();}
  void Seek(const Slice& target) {
    leveldb::Slice seek_key(target.data(), target.size());
    it_->Seek(seek_key);
  }
  void Next() {it_->Next();}
  void Prev() {it_->Prev();}
  Slice key() const {
    leveldb::Slice ret_key = it_->key();
    return Slice(ret_key.data(), ret_key.size());
  }
  Slice pkey() const {
    leveldb::Slice ret_val = it_->value();
    return Slice(ret_val.data(), ret_val.size()); /* NOT IMPLEMENT */
  }
private:
  leveldb::Iterator *it_;
};

class KVIndexLSM : public KVIndex {
public:
  KVIndexLSM (const Options& options, kvssd::KVSSD* kvd, std::string& name);
  ~KVIndexLSM ();

  // implmentations
  bool Put(const Slice& key);
  bool Put(const Slice& skey, const Slice& pkey);
  bool Get(const Slice& skey, std::string& pkey);
  bool Delete(const Slice& key);
  bool Write(IDXWriteBatch* updates);
  IDXIterator* NewIterator(const ReadOptions& options);
 
private:
  std::string name_;
  leveldb::DB* db_;
  leveldb::WriteOptions write_options_;
  leveldb::ReadOptions read_options_;
  kvssd::KVSSD* kvd_;
  ComparatorLSM* cmp_;
};

KVIndexLSM::KVIndexLSM(const Options& db_options, kvssd::KVSSD* kvd, std::string& name) : name_(name), kvd_(kvd) {
  leveldb::Options options;
  options.create_if_missing = true;
  options.max_open_files = 1000;
  options.filter_policy = NULL;
  options.compression = leveldb::kNoCompression;
  options.reuse_logs = false;
  //options.write_buffer_size = 128 << 10;
  //options.max_file_size = 128 << 10;
  options.write_buffer_size = 16 << 20;
  options.max_file_size = 8 << 20;
  options.block_size = 16384;

  // apply db options
  cmp_ = new ComparatorLSM(db_options.comparator);
  options.comparator = cmp_;

  if (db_options.indexType == LSMOPT)
    options.env = leveldb::NewKVEnvOpt(leveldb::Env::Default(), kvd);
  else 
    options.env = leveldb::NewKVEnv(leveldb::Env::Default(), kvd);
  leveldb::Status status = leveldb::DB::Open(options, name, &db_);
}

KVIndexLSM::~KVIndexLSM() {
  delete db_;
  delete cmp_;
}

KVIndex* NewLSMIndex(const Options& options, kvssd::KVSSD* kvd, std::string& name) {
  return new KVIndexLSM(options, kvd, name);
}

IDXWriteBatch* NewIDXWriteBatchLSM() {
  return new IDXWriteBatchLSM();
}

bool KVIndexLSM::Put(const Slice &key) {
  leveldb::Slice put_key(key.data(), key.size());
  leveldb::Slice put_val; // don't care
  return (db_->Put(write_options_, put_key, put_val)).ok();
}

bool KVIndexLSM::Put(const Slice& skey, const Slice& pkey) {
  leveldb::Slice put_key(skey.data(), skey.size());
  leveldb::Slice put_val(pkey.data(), pkey.size()); 
  return (db_->Put(write_options_, put_key, put_val)).ok();
}

bool KVIndexLSM::Get(const Slice& skey, std::string& pkey) { 
  leveldb::Slice get_key(skey.data(), skey.size());
  return (db_->Get(read_options_, get_key, &pkey)).ok();
}

bool KVIndexLSM::Delete(const Slice &key) {
  leveldb::Slice put_key(key.data(), key.size());
  return (db_->Delete(write_options_, put_key)).ok();
}

bool KVIndexLSM::Write(IDXWriteBatch* updates) {
  return (db_->Write(write_options_, (leveldb::WriteBatch*)(updates->InternalBatch()))).ok();
}

IDXIterator* KVIndexLSM::NewIterator(const ReadOptions& options) {
  return new IDXIteratorLSM(db_, options);
}

}  // namespace kvrangedb
