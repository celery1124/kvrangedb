/******* kvrangedb *******/
/* kv_index.h
* 08/04/2019
* by Mian Qin
*/

#include "kv_index.h"
#include "kvrangedb/comparator.h"
#include "base/base.h"

namespace kvrangedb {


class ComparatorBase : public base::Comparator {
public:
  ComparatorBase(const kvrangedb::Comparator* cmp) : cmp_(cmp) {};
  ~ComparatorBase() {};
  int Compare(const base::Slice& a, const base::Slice& b) const {
    Slice aa(a.data(), a.size());
    Slice bb(b.data(), b.size());
    return cmp_->Compare(aa, bb);
  }
private:
  const kvrangedb::Comparator* cmp_;
};

class IDXWriteBatchBase : public IDXWriteBatch {
public: 
  IDXWriteBatchBase () {};
  ~IDXWriteBatchBase () {};
  void Put(const Slice& key) {
    // do nothing
  }
  void Put(const Slice& lkey, const Slice& pkey) {
    // do nothing
  }
  void Delete(const Slice& key) {
    // do nothing
  }
  void Clear() {}
  int Size() {return 0;}
  void *InternalBatch() {return NULL;}
};

class IDXIteratorBase: public IDXIterator {
public:
  IDXIteratorBase(base::BaseOrder *base, const ReadOptions& options) : base_(base) {
    it_ = base_->NewIterator(options.scan_length);
  }
  ~IDXIteratorBase() { 
    delete it_; 
    delete base_;
  }

  bool Valid() const {it_->Valid();}
  void SeekToFirst() {it_->SeekToFirst();}
  void SeekToLast() {
    // NOT IMPLEMENT
  }
  void Seek(const Slice& target) {
    base::Slice seek_key(target.data(), target.size());
    it_->Seek(&seek_key);
  }
  void Next() {it_->Next();}
  void Prev() {
    // NOT IMPLEMENT
  }
  Slice key() const {
    base::Slice ret_key = it_->key();
    return Slice(ret_key.data(), ret_key.size());
  }
  Slice pkey() const {
    return Slice(); /* NOT IMPLEMENT */
  }
private:
  base::BaseOrder* base_;
  base::BaseOrder::Iterator *it_;
};

class KVIndexBase : public KVIndex {
public:
  KVIndexBase (const Options& options, kvssd::KVSSD* kvd, std::string& name);
  ~KVIndexBase ();

  // implmentations
  bool Put(const Slice& key);
  bool Put(const Slice& skey, const Slice& pkey);
  bool Get(const Slice& skey, std::string& pkey);
  bool Delete(const Slice& key);
  bool Write(IDXWriteBatch* updates);
  IDXIterator* NewIterator(const ReadOptions& options);
 
private:
  std::string name_;
  kvssd::KVSSD* kvd_;
  ComparatorBase* cmp_;
};

KVIndexBase::KVIndexBase(const Options& db_options, kvssd::KVSSD* kvd, std::string& name) : name_(name), kvd_(kvd) {
  // apply db options
  cmp_ = new ComparatorBase(db_options.comparator);
}

KVIndexBase::~KVIndexBase() {
  delete cmp_;
}

KVIndex* NewBaseIndex(const Options& options, kvssd::KVSSD* kvd, std::string& name) {
  return new KVIndexBase(options, kvd, name);
}

IDXWriteBatch* NewIDXWriteBatchBase() {
  return new IDXWriteBatchBase();
}

bool KVIndexBase::Put(const Slice &key) {
  // do nothing
  return true;
}

bool KVIndexBase::Put(const Slice &skey, const Slice &pkey) {
  /** NOT IMPLEMENT **/
  return true;
}

bool KVIndexBase::Get(const Slice& skey, std::string& pkey) { 
  /** NOT IMPLEMENT **/
  return true;
}

bool KVIndexBase::Delete(const Slice &key) {
  // do nothing
  return true;
}

bool KVIndexBase::Write(IDXWriteBatch* updates) {
  // do nothing
  return true;
}

IDXIterator* KVIndexBase::NewIterator(const ReadOptions& options) {
  base::BaseOrder* base_ = new base::BaseOrder(cmp_, kvd_);
  return new IDXIteratorBase(base_, options);
}

}  // namespace kvrangedb
