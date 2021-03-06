/******* kvrangedb *******/
/* kv_index_btree.cc
* 07/29/2019
* by Mian Qin
*/

#include "kv_index.h"
#include "kvrangedb/comparator.h"
#include "kvbtree/bplustree.h"
#include "kvbtree/write_batch.h"

namespace kvrangedb {

class ComparatorBTree : public kvbtree::Comparator {
public:
  ComparatorBTree(const kvrangedb::Comparator* cmp) : cmp_(cmp) {};
  ~ComparatorBTree() {};
  int Compare(const kvbtree::Slice& a, const kvbtree::Slice& b) const {
    Slice aa(a.data(), a.size());
    Slice bb(b.data(), b.size());
    return cmp_->Compare(aa, bb);
  }
  
private:
  const kvrangedb::Comparator* cmp_;
};

class IDXWriteBatchBTree : public IDXWriteBatch {
public: 
  IDXWriteBatchBTree () :size_(0) {};
  ~IDXWriteBatchBTree () {};
  void Put(const Slice& key) {
    kvbtree::Slice put_key(key.data(), key.size());
    batch_.Put(put_key);
    size_++;
  }
  void Put(const Slice& lkey, const Slice& pkey) {
    // do nothing
  }
  void Delete(const Slice& key) {
    kvbtree::Slice del_key(key.data(), key.size());
    batch_.Delete(del_key);
    size_--;
  }
  void Clear() {size_ = 0; 
    batch_.Clear();
  }
  int Size() {return size_;}
  void *InternalBatch() {return &batch_;}

public:
  kvbtree::WriteBatch batch_;
  int size_;
};

class IDXIteratorBTree : public IDXIterator {
public:
  IDXIteratorBTree(kvbtree::KVBplusTree* db, const ReadOptions& options) {
    // TODO apply read options to kvbtree iterator
    it_ = db->NewIterator();
  }
  ~IDXIteratorBTree() { delete it_; }

  bool Valid() const {it_->Valid();}
  void SeekToFirst() {it_->SeekToFirst();}
  void SeekToLast() { }
  void Seek(const Slice& target) {
    kvbtree::Slice seek_key(target.data(), target.size());
    it_->Seek(&seek_key);
  }
  void Next() {it_->Next();}
  void Prev() { }
  Slice key() const {
    kvbtree::Slice ret_key = it_->key();
    return Slice(ret_key.data(), ret_key.size());
  }
  Slice pkey() const {
    return Slice(); /* NOT IMPLEMENT */
  }
private:
  kvbtree::KVBplusTree::Iterator *it_;
};

class KVIndexBTree : public KVIndex {
public:
  KVIndexBTree (const Options& options, kvssd::KVSSD* kvd, std::string& name);
  ~KVIndexBTree ();

  // implmentations
  bool Put(const Slice& key);
  bool Put(const Slice& skey, const Slice& pkey);
  bool Get(const Slice& skey, std::string& pkey);
  bool Delete(const Slice& key);
  bool Write(IDXWriteBatch* updates);
  IDXIterator* NewIterator(const ReadOptions& options);
 
private:
  std::string name_;
  kvbtree::KVBplusTree *db_;
  kvssd::KVSSD *kvd_;
  kvbtree::Comparator *cmp_;
};

KVIndexBTree::KVIndexBTree(const Options& db_options, kvssd::KVSSD* kvd, std::string& name) : name_(name), kvd_(kvd) {
  cmp_ = new ComparatorBTree(db_options.comparator);
  db_ = new kvbtree::KVBplusTree(cmp_, kvd, 1024, 4096);
}

KVIndexBTree::~KVIndexBTree() {
  delete db_;
  delete cmp_;
}

KVIndex* NewBTreeIndex(const Options& options, kvssd::KVSSD* kvd, std::string& name) {
  return new KVIndexBTree(options, kvd, name);
}

IDXWriteBatch* NewIDXWriteBatchBTree() {
  return new IDXWriteBatchBTree();
}

bool KVIndexBTree::Put(const Slice &key) {
  kvbtree::Slice put_key(key.data(), key.size());
  return db_->Insert(&put_key);
}

bool KVIndexBTree::Put(const Slice &skey, const Slice &pkey) {
  /** NOT IMPLEMENT **/
  return true;
}

bool KVIndexBTree::Get(const Slice& skey, std::string& pkey) { 
  /** NOT IMPLEMENT **/
  return true;
}

bool KVIndexBTree::Delete(const Slice &key) {
  // NOT IMPLEMENT
}

bool KVIndexBTree::Write(IDXWriteBatch* updates) {
  db_->Write((kvbtree::WriteBatch*)updates->InternalBatch());
}

IDXIterator* KVIndexBTree::NewIterator(const ReadOptions& options) {
  return new IDXIteratorBTree(db_, options);
}

}  // namespace kvrangedb
