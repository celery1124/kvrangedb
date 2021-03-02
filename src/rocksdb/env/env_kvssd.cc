// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "port/port.h"
#include "util/mutexlock.h"
#include "util/coding.h"
#include <chrono>
#include <string>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <atomic>

// #define IO_DBG

namespace rocksdb {
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

struct AsyncDel_context {
  std::atomic<int> *complete;
  std::string *keyStr;
  kvssd::Slice *key;
  AsyncDel_context(std::atomic<int> *_complete, std::string *_keyStr, kvssd::Slice *_key) :
  complete(_complete), keyStr(_keyStr), key(_key) {}
};

static void kv_del_async_cb (void *args) {
    AsyncDel_context *ctx = (AsyncDel_context *)args;
    ctx->complete->fetch_add(1);
    delete ctx->keyStr;
    delete ctx->key;
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
    int kvd_ret = 0;
    if (offset < data_length_) { // read data block
      std::string dblk_name = filename_+'/'+std::to_string(offset);
      kvssd::Slice key (dblk_name);
      //kvd_->kv_get_oneshot(&key, scratch, n);
      char *base;
      int size; 
      size_t align = 4; // 4 byte alignment (kvssd retrieve required!)
      kvd_ret = kvd_->kv_get(&key, base, size, (n + (align - 1)) & ~(align - 1));
      assert(kvd_ret == 0);
      assert((size_t)size == n);
      memcpy(scratch, base, n);
      free(base);
      *result = Slice(scratch, n);
    }
    else { // read meta block
      offset -= data_length_; // meta KV offset
      *result = Slice(reinterpret_cast<char*>(meta_region_) + offset, n);
    }
    return kvd_ret == 0 ? Status::OK() : Status::NotFound();
  }
};

class KVWritableFileOpt : public WritableFile {
 private:
  std::string filename_;
  std::string value_;
  int offset_;
  bool synced_;
  kvssd::KVSSD* kvd_;
  std::vector<int> offsetList_;
  std::vector<Monitor*> monList_;

 public:
  KVWritableFileOpt(kvssd::KVSSD* kvd, const std::string& fname)
      : filename_(fname), offset_(0), synced_(false), kvd_(kvd) {  }

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
    if (!synced_) Sync();
    return Status::OK();
  }

  virtual Status Flush() {
    if (value_.size()) {
      // write block
      std::string *keyStr = new std::string;
      *keyStr += (filename_+'/'+std::to_string(offset_));
      offsetList_.push_back(offset_);
      std::string *valStr = new std::string;
      valStr->swap(value_);
      kvssd::Slice *key = new kvssd::Slice (*keyStr);
      kvssd::Slice *val = new kvssd::Slice (*valStr);
      Monitor *mon = new Monitor;
      AsyncStore_context *ctx = new AsyncStore_context(mon, keyStr, valStr, key, val);
#ifdef IO_DBG
      printf("[env_writable] %s (%d)\n", keyStr->c_str(), (int)valStr->size());
#endif
      kvd_->kv_store_async(key, val, kv_store_async_cb, (void*)ctx);
      monList_.push_back(mon);

      offset_ += valStr->size();
    }
    return Status::OK();
  }

  Status FlushMeta() {
    // write meta block (embed 4B footer as the datablock size)
    int metaSize = offsetList_.size()*sizeof(int) + sizeof(int);
    char *metaBuf = new char[metaSize];
    char *buf = metaBuf;
    for (unsigned int i = 0; i < offsetList_.size(); i++) {
      EncodeFixed32(buf, offsetList_[i]);
      buf += sizeof(int);
    }
    EncodeFixed32(buf, offset_);
    value_.append(metaBuf, metaSize);
    delete [] metaBuf;
    std::string *keyStr = new std::string;
    *keyStr += (filename_+"/meta");
    std::string *valStr = new std::string;
    valStr->swap(value_);
    kvssd::Slice *key = new kvssd::Slice (*keyStr);
    kvssd::Slice *val = new kvssd::Slice (*valStr);
    Monitor *mon = new Monitor;
    AsyncStore_context *ctx = new AsyncStore_context(mon, keyStr, valStr, key, val);
#ifdef IO_DBG
    printf("[env_writable] %s (%d), sst size %d\n", keyStr->c_str(), (int)valStr->size(), (int)offset_);
#endif
    if(valStr->size() > 3000000) exit(1);
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
    synced_ = true;
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
    
#ifdef IO_DBG
    printf("[env_appendable]: %s (%d)\n",filename_.c_str(), (int)value_.size());
#endif
    if (filename_.find("MANIFEST") != std::string::npos) {
      // manifest can be exceed 2MB (max value size)
      const int MAX_V_SIZE = 2<<20; // 2MB max value size
      char *p = &value_[0];
      int write_bytes = 0;
      int kv_cnt = 0;
      while(write_bytes < (int)value_.size()) {
        int vlen = value_.size() - write_bytes > MAX_V_SIZE ? MAX_V_SIZE : value_.size() - write_bytes;
        std::string key_str = filename_+"_"+std::to_string(kv_cnt++);
        kvssd::Slice key(key_str);
        kvssd::Slice val(p, vlen);
        kvd_->kv_store(&key, &val);
        p += vlen;
        write_bytes += vlen;
      }
    }
    else { // option, log, etc
      kvssd::Slice key (filename_);
      kvssd::Slice val (value_);   
      kvd_->kv_store(&key, &val);
    }

    synced = true;
    return Status::OK();
  }
};

class KVEnvDirectory : public Directory {
 public:
  virtual Status Fsync() override { return Status::OK(); }
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
                                   unique_ptr<SequentialFile>* result,
                                 const EnvOptions& options) {
    *result = NULL;
    char *vbuf;
    int vsize;
    char *base = NULL;
    int size = 0;

    if (fname.find("MANIFEST") != std::string::npos) {
      // read sequential file, may contain multiple kv pairs
      int kv_cnt = 0;
      while (true) {
        std::string key_str = fname+"_"+std::to_string(kv_cnt++);
        kvssd::Slice key(key_str);
        int found = kvd_->kv_get(&key, vbuf, vsize); 
        if (found == 0) {
          base = (char*) realloc(base, size+vsize);
          memcpy(base + size, vbuf, vsize);
          size += vsize;
          free(vbuf);
        }
        else {
          free(vbuf);
          break;
        }
      }
    }
    else {
      kvssd::Slice key (fname);	    
      kvd_->kv_get(&key, base, size);
    }


    //*result = new KVSequentialFileOpt (fname, base, size);
    result->reset(new KVSequentialFileOpt(fname, base, size));
    return Status::OK();
  }

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     unique_ptr<RandomAccessFile>* result,
                                 const EnvOptions& options) {
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
    //*result = new KVRandomAccessFileOpt (kvd_, fname, base, size, dataLen);
    result->reset(new KVRandomAccessFileOpt(kvd_, fname, base, size, dataLen));
    return kvd_ret == 0 ? Status::OK() : Status::NotFound();
  }

  virtual Status NewWritableFile(const std::string& fname,
                                unique_ptr<WritableFile>* result,
                                 const EnvOptions& options) {
    //*result = new KVWritableFileOpt (kvd_, fname);
    result->reset(new KVWritableFileOpt(kvd_, fname));
    return Status::OK();
  }

   virtual Status NewAppendableFile(const std::string& fname,
                                unique_ptr<WritableFile>* result,
                                 const EnvOptions& options) {
    //*result = new KVAppendableFileOpt (kvd_, fname);
    result->reset(new KVAppendableFileOpt(kvd_, fname));
    return Status::OK();
  }

  virtual Status FileExists(const std::string& fname) {
    kvssd::Slice key(fname);
    bool found = kvd_->kv_exist(&key);
    return found ? Status::OK() : Status::NotFound();
  }

  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) {
    return Status::OK();                           
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
    if (fname.find("sst") != std::string::npos) {
      int submitted = 0;
      std::atomic<int> completed(0);

      char *base;
      int size, offset;
      // read meta KV 
      std::string *meta_name = new std::string(fname+"/meta");
        kvssd::Slice *meta_key = new kvssd::Slice (*meta_name);
      int kvd_ret = kvd_->kv_get(meta_key, base, size, 16<<10 /*16KB*/);
      assert(kvd_ret == 0);
      if (kvd_ret != 0) {
#ifdef IO_DBG
        printf("[env_delete] %s failed\n", fname.c_str());
#endif
        return Status::IOError(fname, "KV not found");
      }
      // read offset list and delete block
      char *p = base + size - sizeof(int);
      while (true) {
        offset = DecodeFixed32(p);
        std::string *block_name = new std::string(fname+'/'+std::to_string(offset));
        kvssd::Slice *block_key = new kvssd::Slice (*block_name);
        AsyncDel_context *ctx = new AsyncDel_context(&completed, block_name, block_key);
        kvd_->kv_delete_async(block_key, kv_del_async_cb, (void*)ctx);
        submitted++;
        p -= sizeof(int);
        if (offset == 0) break;
      }
      AsyncDel_context *ctx = new AsyncDel_context(&completed, meta_name, meta_key);
      kvd_->kv_delete_async(meta_key, kv_del_async_cb, (void*)ctx);
      submitted++;
#ifdef IO_DBG
      printf("[env_delete] %s successed\n", fname.c_str());
#endif
      
      // wait until commands that has succussfully submitted finish
      while(completed < submitted) {
        usleep(1);
      }
      return Status::OK();
    }
    else {
      printf("[env_delete] %s failed\n", fname.c_str());
      return Status::IOError(fname, "KV not found");
    }
    
  }

  virtual Status CreateDir(const std::string& dirname) {
    return Status::OK();
  }

  virtual Status CreateDirIfMissing(const std::string& dirname) {
  CreateDir(dirname);
  return Status::OK();
  }

  virtual Status DeleteDir(const std::string& dirname) {
    return Status::OK();
  }

  virtual Status GetFileSize(const std::string& fname, uint64_t* file_size) {
    kvssd::Slice key(fname);
    if (fname.find("MANIFEST") != std::string::npos) {
      int kv_cnt = 0;
      int fsize = 0;
      while (true) {
        std::string key_str = fname+"_"+std::to_string(kv_cnt++);
        kvssd::Slice mkey(key_str);

        if (kvd_->kv_exist(&mkey)) {
          fsize += kvd_->kv_get_size(&mkey);
        }
        else {
          break;
        }
      }
      *file_size = fsize;
    }
    else {
      if (!kvd_->kv_exist(&key)) {
        return Status::IOError(fname, "KV not found");
      }
      *file_size = kvd_->kv_get_size(&key);
    }
    
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
    
    //printf("[env_rename] from %s to %s\n", src.c_str(), target.c_str());
    return Status::OK();
  }

  virtual Status LockFile(const std::string& fname, FileLock** lock) {
    return Status::OK();
  }

  virtual Status UnlockFile(FileLock* lock) {
    return Status::OK();
  }

  virtual Status NewDirectory(const std::string& name,
                                unique_ptr<Directory>* result) {
  //*result = nullptr;
  result->reset(new KVEnvDirectory());
  return Status::OK();
}

  virtual Status GetTestDirectory(std::string* path) {
    *path = "/test";
    return Status::OK();
  }

  virtual Status NewLogger(const std::string& fname, shared_ptr<Logger>* result) {
    *result = nullptr;
    return Status::OK();
  }

  virtual double GetDevUtil() {
    // double res = kvd_->get_util();
    // printf("getdevutil %.3f\n", res);
    return (double)kvd_->get_util();
  }
};

/*****  KVSSDEnvOpt End  *****/
Env* NewKVEnvOpt(Env* base_env, kvssd::KVSSD* kvd) {
  return new KVSSDEnvOpt(base_env, kvd);
}

}  // namespace rocksdb
