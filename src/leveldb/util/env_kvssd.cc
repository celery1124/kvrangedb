// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/env.h"
#include "leveldb/status.h"
#include "port/port.h"
#include "util/coding.h"
#include <string>
#include <vector>
#include <mutex>
#include <condition_variable>

namespace leveldb {

/*****  KVSSDEnv  *****/
class KVSequentialFile: public SequentialFile {
 private:
  std::string filename_;
  void *mapped_region_;
  size_t length_;
  size_t offset_;

 public:
  KVSequentialFile(const std::string& fname, void *base, size_t length)
      : filename_(fname),mapped_region_(base), length_(length), offset_(0) {
  }

  virtual ~KVSequentialFile() {
    free(mapped_region_);
  }

  // n is aligned with 64B
  virtual Status Read(size_t n, Slice* result, char* scratch) {
    Status s;
    if (offset_ + n > length_) { // return rest of the region
      *result = Slice(reinterpret_cast<char*>(mapped_region_) + offset_, length_ - offset_);
      offset_ = length_;
      //s = IOError(filename_, EINVAL);
    } else {
      *result = Slice(reinterpret_cast<char*>(mapped_region_) + offset_, n);
      offset_ += n;
    }
    return s;
  }
  virtual Status Skip(uint64_t n) {
    offset_ += n;
    return Status::OK();
  }
};

class KVRandomAccessFile: public RandomAccessFile {
 private:
  std::string filename_;
  void* mapped_region_;
  size_t length_;

 public:
  KVRandomAccessFile(const std::string& fname, void* base, size_t length)
      : filename_(fname), mapped_region_(base), length_(length) {
  }

  virtual ~KVRandomAccessFile() {
    free(mapped_region_);
  }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    Status s;
    //fprintf(stderr, "read %s, offset %d, n %d\n", filename_.c_str(), offset, n);
    if (offset + n > length_) { // return rest of the region
      *result = Slice(reinterpret_cast<char*>(mapped_region_) + offset, length_ - offset);
      //s = IOError(filename_, EINVAL);
    } else {
      *result = Slice(reinterpret_cast<char*>(mapped_region_) + offset, n);
    }
    return s;
  }
};

class KVWritableFile : public WritableFile {
 private:
  std::string filename_;
  std::string value_;
  kvssd::KVSSD* kvd_;
  bool synced;

 public:
  KVWritableFile(kvssd::KVSSD* kvd, const std::string& fname)
      : filename_(fname), kvd_(kvd), synced(false) {  }

  ~KVWritableFile() { }

  virtual Status Append(const Slice& data) {
    value_.append(data.data(), data.size());
    return Status::OK();
  }

  virtual Status Reset() {
    value_.clear();
    return Status::OK();
  }

  virtual Status Close() {
    if (!synced) Sync();
    return Status::OK();
  }

  virtual Status Flush() {
    return Status::OK();
  }

  virtual Status Sync() {
    // Ensure new files referred to by the manifest are in the filesystem.
    //Status s = SyncDirIfManifest();
    kvssd::Slice key (filename_);
    kvssd::Slice val (value_);
    kvd_->kv_store(&key, &val);
    synced = true;
    //printf("KVWritable: %s, size %d bytes\n",filename_.c_str(), val.size());
    //fprintf(stderr, "KVWritable: %s, size %d bytes\n",filename_.c_str(), val.size());
    return Status::OK();
  }
};

class KVAppendableFile : public WritableFile {
 private:
  std::string filename_;
  std::string value_;
  kvssd::KVSSD* kvd_;
  bool synced;

 public:
  KVAppendableFile(kvssd::KVSSD* kvd, const std::string& fname)
      : filename_(fname), kvd_(kvd), synced(false) {  }

  ~KVAppendableFile() { }

  virtual Status Append(const Slice& data) {
    value_.append(data.data(), data.size());
    return Status::OK();
  }

  virtual Status Reset() {
    value_.clear();
    return Status::OK();
  }

  virtual Status Close() {
    if (!synced) Sync();
    return Status::OK();
  }

  virtual Status Flush() {
    return Status::OK();
  }

  virtual Status Sync() {
    // Ensure new files referred to by the manifest are in the filesystem.
    //Status s = SyncDirIfManifest();
    kvssd::Slice key (filename_);
    kvssd::Slice val (value_);
    kvd_->kv_store(&key, &val);
    //kvd_->kv_append(&key, &val);
    //printf("append: %s\n",filename_.c_str());
    synced = true;
    return Status::OK();
  }
};

class KVSSDEnv : public EnvWrapper {
  private:
    kvssd::KVSSD* kvd_;
  public:
  explicit KVSSDEnv(Env* base_env, kvssd::KVSSD* kvd) 
  : EnvWrapper(base_env), kvd_(kvd) {
  }
  virtual ~KVSSDEnv() { }

  // Partial implementation of the Env interface.
  virtual Status NewSequentialFile(const std::string& fname,
                                   SequentialFile** result) {
    *result = NULL;
    char * base;
    int size;
    kvssd::Slice key (fname);
    kvd_->kv_get(&key, base, size);
    *result = new KVSequentialFile (fname, base, size);
    return Status::OK();
  }

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     RandomAccessFile** result) {
    *result = NULL;
    char * base;
    int size;
    kvssd::Slice key (fname);
    kvd_->kv_get(&key, base, size);
    *result = new KVRandomAccessFile (fname, base, size);
    //fprintf(stderr, "%s, %d\n", fname.c_str(), size);
    return Status::OK();
  }

  virtual Status NewWritableFile(const std::string& fname,
                                 WritableFile** result) {
    *result = new KVWritableFile(kvd_, fname);
    return Status::OK();
  }

  virtual Status NewAppendableFile(const std::string& fname,
                                   WritableFile** result) {
    *result = new KVAppendableFile(kvd_, fname);
    return Status::OK();
  }

  virtual bool FileExists(const std::string& fname) {
    kvssd::Slice key(fname);
    return kvd_->kv_exist(&key);
  }

  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) {
    result->clear();
    std::vector<std::string> keys;
    kvd_->kv_scan_keys(keys);

    for (auto it = keys.begin(); it != keys.end(); ++it){
      const std::string& filename = *it;
      if (filename.size() >= dir.size() + 1 && filename[dir.size()] == '/' &&
          Slice(filename).starts_with(Slice(dir))) {
        result->push_back(filename.substr(dir.size() + 1));
      }
    }

    return Status::OK();
  }

  virtual Status DeleteFile(const std::string& fname) {
    kvssd::Slice key(fname);
    if (!kvd_->kv_exist(&key)) {
      return Status::IOError(fname, "KV not found");
    }
    kvd_->kv_delete(&key);
    return Status::OK();
  }

  virtual Status CreateDir(const std::string& dirname) {
    return Status::OK();
  }

  virtual Status DeleteDir(const std::string& dirname) {
    return Status::OK();
  }

  virtual Status GetFileSize(const std::string& fname, uint64_t* file_size) {
    kvssd::Slice key(fname);
    if (!kvd_->kv_exist(&key)) {
      return Status::IOError(fname, "KV not found");
    }

    *file_size = kvd_->kv_get_size(&key);
    return Status::OK();
  }

  // expensive, probably don't using it
  virtual Status RenameFile(const std::string& src,
                            const std::string& target) {
    kvssd::Slice key_src(src);
    if (!kvd_->kv_exist(&key_src)) {
      return Status::IOError(src, "KV not found");
    }
    kvssd::Slice key_target(target);
    char *vbuf;
    int vlen;
    kvd_->kv_get(&key_src, vbuf, vlen);
    kvssd::Slice val_src (vbuf, vlen);
    kvd_->kv_store(&key_target, &val_src);
    kvd_->kv_delete(&key_src);

    return Status::OK();
  }

  virtual Status LockFile(const std::string& fname, FileLock** lock) {
    return Status::OK();
  }

  virtual Status UnlockFile(FileLock* lock) {
    return Status::OK();
  }

  virtual Status GetTestDirectory(std::string* path) {
    *path = "/test";
    return Status::OK();
  }

  virtual Status NewLogger(const std::string& fname, Logger** result) {
    *result = NULL;
    return Status::OK();
  }
};
/*****  KVSSDEnv End  *****/


/*****  KVSSDEnvOpt  *****/
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

struct AsyncStore_context {
  Monitor *mon;
  std::string *keyStr;
  std::string *valStr;
  kvssd::Slice *key;
  kvssd::Slice *val;
  AsyncStore_context(Monitor *_mon, std::string *_keyStr, std::string *_valStr, kvssd::Slice *_key, kvssd::Slice *_val) :
  mon(_mon), keyStr(_keyStr), valStr(_valStr), key(_key), val(_val) {}
};

static void kv_store_async_cb (void *args) {
    AsyncStore_context *ctx = (AsyncStore_context *)args;
    ctx->mon->notify();
    delete ctx->keyStr;
    delete ctx->valStr;
    delete ctx->key;
    delete ctx->val;
    delete ctx;
}

class KVSequentialFileOpt: public SequentialFile {
 private:
  std::string filename_;
  void *mapped_region_;
  size_t length_;
  size_t offset_;

 public:
  KVSequentialFileOpt(const std::string& fname, void *base, size_t length)
      : filename_(fname),mapped_region_(base), length_(length), offset_(0) {
  }

  virtual ~KVSequentialFileOpt() {
    free(mapped_region_);
  }

  // n is aligned with 64B
  virtual Status Read(size_t n, Slice* result, char* scratch) {
    Status s;
    if (offset_ + n > length_) { // return rest of the region
      *result = Slice(reinterpret_cast<char*>(mapped_region_) + offset_, length_ - offset_);
      offset_ = length_;
      //s = IOError(filename_, EINVAL);
    } else {
      *result = Slice(reinterpret_cast<char*>(mapped_region_) + offset_, n);
      offset_ += n;
    }
    return s;
  }
  virtual Status Skip(uint64_t n) {
    offset_ += n;
    return Status::OK();
  }
};

class KVRandomAccessFileOpt: public RandomAccessFile {
 private:
  kvssd::KVSSD* kvd_;
  std::string filename_;
  void* meta_region_;
  size_t meta_length_;
  size_t data_length_;

 public:
  KVRandomAccessFileOpt(kvssd::KVSSD* kvd, const std::string& fname, void* base, size_t metaLen, size_t dataLen)
      :kvd_(kvd), filename_(fname), meta_region_(base), meta_length_(metaLen), data_length_(dataLen) {
  }

  virtual ~KVRandomAccessFileOpt() {
    free(meta_region_);
  }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    //fprintf(stderr, "read %s, offset %d, n %d\n", filename_.c_str(), offset, n);
    if (offset < data_length_) { // read data block
      std::string dblk_name = filename_+'/'+std::to_string(offset);
      kvssd::Slice key (dblk_name);
      //kvd_->kv_get_oneshot(&key, scratch, n);
      char *base;
      int size; 
      size_t align = 4; // 4 byte alignment (kvssd retrieve required!)
      int kvd_ret = kvd_->kv_get(&key, base, size, (n + (align - 1)) & ~(align - 1));
      assert(kvd_ret == 0);
      assert(size == n);
      memcpy(scratch, base, n);
      free(base);
      *result = Slice(scratch, n);
    }
    else { // read meta block
      offset -= data_length_; // meta KV offset
      *result = Slice(reinterpret_cast<char*>(meta_region_) + offset, n);
    }
    return Status::OK();
  }
};

class KVWritableFileOpt : public WritableFile {
 private:
  std::string filename_;
  std::string value_;
  int offset_;
  kvssd::KVSSD* kvd_;
  std::vector<Monitor*> monList_;

 public:
  KVWritableFileOpt(kvssd::KVSSD* kvd, const std::string& fname)
      : filename_(fname), kvd_(kvd), offset_(0) {  }

  ~KVWritableFileOpt() { }

  virtual Status Append(const Slice& data) {
    value_.append(data.data(), data.size());
    return Status::OK();
  }

  virtual Status Reset() {
    value_.clear();
    return Status::OK();
  }

  virtual Status Close() {
    assert(value_.size() == 0);
    return Status::OK();
  }

  virtual Status Flush() {
    // write block
    std::string *keyStr = new std::string;
    *keyStr += (filename_+'/'+std::to_string(offset_));
    std::string *valStr = new std::string;
    valStr->swap(value_);
    kvssd::Slice *key = new kvssd::Slice (*keyStr);
    kvssd::Slice *val = new kvssd::Slice (*valStr);
    Monitor *mon = new Monitor;
    AsyncStore_context *ctx = new AsyncStore_context(mon, keyStr, valStr, key, val);
    kvd_->kv_store_async(key, val, kv_store_async_cb, (void*)ctx);
    monList_.push_back(mon);

    offset_ += valStr->size();
    return Status::OK();
  }

  Status FlushMeta() {
    // write meta block (embed 4B footer as the datablock size)
    char buf[sizeof(offset_)];
    EncodeFixed32(buf, offset_);
    value_.append(buf, sizeof(buf));
    std::string *keyStr = new std::string;
    *keyStr += (filename_+"/meta");
    std::string *valStr = new std::string;
    valStr->swap(value_);
    kvssd::Slice *key = new kvssd::Slice (*keyStr);
    kvssd::Slice *val = new kvssd::Slice (*valStr);
    Monitor *mon = new Monitor;
    AsyncStore_context *ctx = new AsyncStore_context(mon, keyStr, valStr, key, val);
    kvd_->kv_store_async(key, val, kv_store_async_cb, (void*)ctx);
    //kvd_->kv_store(key, val);
    monList_.push_back(mon);

    offset_ += valStr->size();
    return Status::OK();
  }

  virtual Status Sync() {
    // Ensure remain meta data is written
    FlushMeta();

    // Wait until all async I/O finish
    for (auto it = monList_.begin(); it != monList_.end(); ++it) {
      (*it)->wait();
      delete (*it);
    }
    //fprintf(stderr, "KVWritable: %s, size %d bytes\n",filename_.c_str(), offset_);
    return Status::OK();
  }
};

class KVAppendableFileOpt : public WritableFile {
 private:
  std::string filename_;
  std::string value_;
  kvssd::KVSSD* kvd_;
  bool synced;

 public:
  KVAppendableFileOpt(kvssd::KVSSD* kvd, const std::string& fname)
      : filename_(fname), kvd_(kvd), synced(false) {  }

  ~KVAppendableFileOpt() { }

  virtual Status Append(const Slice& data) {
    value_.append(data.data(), data.size());
    return Status::OK();
  }

  virtual Status Reset() {
    value_.clear();
    return Status::OK();
  }

  virtual Status Close() {
    if (!synced) Sync();
    return Status::OK();
  }

  virtual Status Flush() {
    return Status::OK();
  }

  virtual Status Sync() {
    // Ensure new files referred to by the manifest are in the filesystem.
    //Status s = SyncDirIfManifest();
    kvssd::Slice key (filename_);
    kvssd::Slice val (value_);
    kvd_->kv_store(&key, &val);
    //kvd_->kv_append(&key, &val);
    //printf("append: %s\n",filename_.c_str());
    synced = true;
    return Status::OK();
  }
};

class KVSSDEnvOpt : public EnvWrapper {
  private:
    kvssd::KVSSD* kvd_;
  public:
  explicit KVSSDEnvOpt(Env* base_env, kvssd::KVSSD* kvd) 
  : EnvWrapper(base_env), kvd_(kvd) {
  }
  virtual ~KVSSDEnvOpt() { }

  // Partial implementation of the Env interface.
  virtual Status NewSequentialFile(const std::string& fname,
                                   SequentialFile** result) {
    *result = NULL;
    char * base;
    int size;
    kvssd::Slice key (fname);
    kvd_->kv_get(&key, base, size);
    *result = new KVSequentialFileOpt (fname, base, size);
    return Status::OK();
  }

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     RandomAccessFile** result) {
    *result = NULL;
    char * base;
    int size, dataLen;
    // read meta KV on NewRandomAccessFile construction
    std::string meta_name = fname+"/meta";
    kvssd::Slice key (meta_name);
    int kvd_ret = kvd_->kv_get(&key, base, size, 16<<10 /*16KB*/);
    assert(kvd_ret == 0);
    // read footer for data block size
    dataLen = DecodeFixed32(base+size-sizeof(uint32_t));
    //fprintf(stderr, "%s, %d\n", fname.c_str(), dataLen + size - sizeof(uint32_t));
    *result = new KVRandomAccessFileOpt (kvd_, fname, base, size, dataLen);
    return Status::OK();
  }

  virtual Status NewWritableFile(const std::string& fname,
                                 WritableFile** result) {
    *result = new KVWritableFileOpt (kvd_, fname);
    return Status::OK();
  }

  virtual Status NewAppendableFile(const std::string& fname,
                                   WritableFile** result) {
    *result = new KVAppendableFileOpt (kvd_, fname);
    return Status::OK();
  }

  virtual bool FileExists(const std::string& fname) {
    kvssd::Slice key(fname);
    return kvd_->kv_exist(&key);
  }

  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) {
    result->clear();
    std::vector<std::string> keys;
    kvd_->kv_scan_keys(keys);

    for (auto it = keys.begin(); it != keys.end(); ++it){
      const std::string& filename = *it;
      if (filename.size() >= dir.size() + 1 && filename[dir.size()] == '/' &&
          Slice(filename).starts_with(Slice(dir))) {
        result->push_back(filename.substr(dir.size() + 1));
      }
    }

    return Status::OK();
  }

  virtual Status DeleteFile(const std::string& fname) {
    kvssd::Slice key(fname);
    if (!kvd_->kv_exist(&key)) {
      return Status::IOError(fname, "KV not found");
    }
    kvd_->kv_delete(&key);
    return Status::OK();
  }

  virtual Status CreateDir(const std::string& dirname) {
    return Status::OK();
  }

  virtual Status DeleteDir(const std::string& dirname) {
    return Status::OK();
  }

  virtual Status GetFileSize(const std::string& fname, uint64_t* file_size) {
    kvssd::Slice key(fname);
    if (!kvd_->kv_exist(&key)) {
      return Status::IOError(fname, "KV not found");
    }

    *file_size = kvd_->kv_get_size(&key);
    return Status::OK();
  }

  // expensive, probably don't using it
  virtual Status RenameFile(const std::string& src,
                            const std::string& target) {
    kvssd::Slice key_src(src);
    if (!kvd_->kv_exist(&key_src)) {
      return Status::IOError(src, "KV not found");
    }
    kvssd::Slice key_target(target);
    char *vbuf;
    int vlen;
    kvd_->kv_get(&key_src, vbuf, vlen);
    kvssd::Slice val_src (vbuf, vlen);
    kvd_->kv_store(&key_target, &val_src);
    kvd_->kv_delete(&key_src);

    return Status::OK();
  }

  virtual Status LockFile(const std::string& fname, FileLock** lock) {
    return Status::OK();
  }

  virtual Status UnlockFile(FileLock* lock) {
    return Status::OK();
  }

  virtual Status GetTestDirectory(std::string* path) {
    *path = "/test";
    return Status::OK();
  }

  virtual Status NewLogger(const std::string& fname, Logger** result) {
    *result = NULL;
    return Status::OK();
  }
};
/*****  KVSSDEnvOpt End  *****/

Env* NewKVEnv(Env* base_env, kvssd::KVSSD* kvd) {
  return new KVSSDEnv(base_env, kvd);
}

Env* NewKVEnvOpt(Env* base_env, kvssd::KVSSD* kvd) {
  return new KVSSDEnvOpt(base_env, kvd);
}

}  // namespace leveldb
