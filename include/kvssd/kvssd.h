
#ifndef _kvssd_h_
#define _kvssd_h_

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <sstream>
#include <atomic>
#include <vector>
#include <semaphore.h>
#include "slice.h"
#include "kvs_api.h"

#include "kvrangedb/statistics.h"

#define DEV_Q_DEPTH 1024
#define INIT_GET_BUFF 65536 // 64KB
using namespace kvrangedb;

namespace kvssd {
  
  class KVSSD;
  class KVSSD_SD;
  struct Async_get_context {
    KVSSD *dev;
    char*& vbuf;
    uint32_t& actual_len;
    void* args;
    Async_get_context(KVSSD *_dev, char *&_buf, uint32_t &_len, void *_args)
    : dev(_dev), vbuf(_buf), actual_len(_len), args(_args) {};
  } ;

  struct Async_sd_get_context {
    KVSSD_SD *dev;
    char*& vbuf;
    uint32_t& actual_len;
    void* args;
    Async_sd_get_context(KVSSD_SD *_dev, char *&_buf, uint32_t &_len, void *_args)
    : dev(_dev), vbuf(_buf), actual_len(_len), args(_args) {};
  } ;

  class KVSSD_SD {
    private:
      char kvs_dev_path[32];
      kvs_init_options options;
      kvs_device_handle dev;
      kvs_container_context ctx;
      kvs_container_handle cont_handle;

      friend class kv_iter;
    public:
      sem_t q_sem;
      Statistics *statistics;
      KVSSD_SD(const char* dev_path, Statistics *stats) : statistics(stats) {
        sem_init(&q_sem, 0, DEV_Q_DEPTH);
        memset(kvs_dev_path, 0, 32);
        memcpy(kvs_dev_path, dev_path, strlen(dev_path));
        kvs_init_env_opts(&options);
        options.memory.use_dpdk = 0;
        // options for asynchronized call
        options.aio.iocoremask = 0;
        options.aio.queuedepth = DEV_Q_DEPTH;

        const char *configfile = "kvssd_emul.conf";
        options.emul_config_file =  configfile;
        kvs_init_env(&options);

        kvs_open_device(dev_path, &dev);
        kvs_create_container(dev, "test", 4, &ctx);
        if (kvs_open_container(dev, "test", &cont_handle) == KVS_ERR_CONT_NOT_EXIST) {
          kvs_create_container(dev, "test", 4, &ctx);
          kvs_open_container(dev, "test", &cont_handle);
        }
      }
      ~KVSSD_SD() {
        sem_destroy(&q_sem);
        RecordTick(statistics, DEV_UTIL, get_util());
        kvs_close_container(cont_handle);
        kvs_close_device(dev);

      }
      bool kv_exist (const Slice *key);
      uint32_t kv_get_size(const Slice *key);
      kvs_result kv_store(const Slice *key, const Slice *val);
      kvs_result kv_store_async(Slice *key, Slice *val, void (*callback)(void *), void *args);
      kvs_result kv_append(const Slice *key, const Slice *val);
      kvs_result kv_append_async(const Slice *key, const Slice *val, void (*callback)(void *), void *args);
      // caller must free vbuf memory
      kvs_result kv_get_oneshot(const Slice *key, char* vbuf, int vlen);
      kvs_result kv_get(const Slice *key, char*& vbuf, int& vlen, int init_size = INIT_GET_BUFF);
      kvs_result kv_get_async(const Slice *key, void (*callback)(void *), void *args);
      kvs_result kv_pget(const Slice *key, char*& vbuf, int count, int offset);
      kvs_result kv_delete(const Slice *key);
      kvs_result kv_delete_async(const Slice *key, void (*callback)(void *), void *args);

      kvs_result kv_scan_keys(std::vector<std::string> &keys, int buf_size = 32768);

      int get_dev_util() {
        int dev_util;
        kvs_get_device_utilization(dev, &dev_util);
        return dev_util;
      }

      int64_t get_capacity() {
        int64_t dev_cap;
        kvs_get_device_capacity(dev, &dev_cap);
        return dev_cap;
      }

      double get_util() {
        return double(get_dev_util())/1000000;
      }


      class kv_iter {
      public:
        int buf_size_;
        uint8_t *buffer_;
        struct iterator_info *iter_info;
      public:
        kv_iter(int buf_size = 32768); // current kvssd iter buffer size is fixed 32KB
        ~kv_iter() { if(buffer_) free(buffer_); }
        int get_num_entries (); 
      };
      bool kv_iter_open(kv_iter *iter);
      bool kv_iter_next(kv_iter *iter); // true-continue, false-end
      bool kv_iter_close(kv_iter *iter);

  };


  class KVSSD_MD {
    private:
      int dev_num_;
      KVSSD_SD **dev_list_;

      friend class kv_iter;
    public:
      Statistics *statistics;
      KVSSD_MD(std::vector<std::string>& devs, Statistics *stats) : statistics(stats) {
        dev_num_ = devs.size();
        dev_list_ = new KVSSD_SD*[dev_num_];
        for (int i = 0; i < dev_num_; i++) {
          dev_list_[i] = new KVSSD_SD(devs[i].c_str(), stats);
        }
        
      }
      ~KVSSD_MD() {
        for (int i = 0; i < dev_num_; i++) {
          delete dev_list_[i];
        }
        delete dev_list_;
      }
      bool kv_exist (const Slice *key);
      uint32_t kv_get_size(const Slice *key);
      kvs_result kv_store(const Slice *key, const Slice *val);
      kvs_result kv_store_async(Slice *key, Slice *val, void (*callback)(void *), void *args);
      kvs_result kv_append(const Slice *key, const Slice *val);
      kvs_result kv_append_async(const Slice *key, const Slice *val, void (*callback)(void *), void *args);
      // caller must free vbuf memory
      kvs_result kv_get_oneshot(const Slice *key, char* vbuf, int vlen);
      kvs_result kv_get(const Slice *key, char*& vbuf, int& vlen, int init_size = INIT_GET_BUFF);
      kvs_result kv_get_async(const Slice *key, void (*callback)(void *), void *args);
      kvs_result kv_pget(const Slice *key, char*& vbuf, int count, int offset);
      kvs_result kv_delete(const Slice *key);
      kvs_result kv_delete_async(const Slice *key, void (*callback)(void *), void *args);
      kvs_result kv_scan_keys(std::vector<std::string> &keys, int buf_size = 32768);

      int get_dev_util() {
        int dev_util = 0;
        for (int i = 0; i < dev_num_; i++) {
          dev_util += dev_list_[i]->get_dev_util();
        }
        return dev_util/dev_num_; // average on all devs
      }

      int64_t get_capacity() {
        int64_t dev_cap = 0;
        for (int i = 0; i < dev_num_; i++) {
          dev_cap += dev_list_[i]->get_capacity();
        }
        return dev_cap;
      }

      double get_util() {
        return double(get_dev_util())/1000000;
      }


      class kv_iter {
      public:
        KVSSD_SD::kv_iter **iters_;
        int buf_size_;
        int buf_entries_;
        uint8_t *buffer_;
      public:
        kv_iter(int buf_size = 32768); // current kvssd iter buffer size is fixed 32KB
        ~kv_iter() { if(buffer_) free(buffer_); }
        int get_num_entries (); 
      };
      bool kv_iter_open(kv_iter *iter);
      bool kv_iter_next(kv_iter *iter); // true-continue, false-end
      bool kv_iter_close(kv_iter *iter);

  };

  class KVSSD {
    private:
      int dev_num_;
      KVSSD_SD *dev_sd_;
      KVSSD_MD *dev_md_;

      friend class kv_iter;
    public:
      Statistics *statistics;
      KVSSD(const char* dev_path, Statistics *stats) : statistics(stats) {
        std::string dev_str(dev_path);
        std::string buf;
        std::stringstream ss(dev_str);       
        std::vector<std::string> devs; // Create vector to hold dev_path
        while (ss >> buf)
            devs.push_back(buf);

        dev_num_ = devs.size();
        if (dev_num_ == 1) {
          dev_sd_ = new KVSSD_SD(dev_path, stats);
          dev_md_ = NULL;
        } else {
          dev_sd_ = NULL;
          dev_md_ = new KVSSD_MD(devs, stats);
        }
        
      }
      ~KVSSD() {
        if (dev_sd_) delete dev_sd_;
        if (dev_md_) delete dev_md_;
      }
      bool kv_exist (const Slice *key);
      uint32_t kv_get_size(const Slice *key);
      kvs_result kv_store(const Slice *key, const Slice *val);
      kvs_result kv_store_async(Slice *key, Slice *val, void (*callback)(void *), void *args);
      kvs_result kv_append(const Slice *key, const Slice *val);
      kvs_result kv_append_async(const Slice *key, const Slice *val, void (*callback)(void *), void *args);
      // caller must free vbuf memory
      kvs_result kv_get_oneshot(const Slice *key, char* vbuf, int vlen);
      kvs_result kv_get(const Slice *key, char*& vbuf, int& vlen, int init_size = INIT_GET_BUFF);
      kvs_result kv_get_async(const Slice *key, void (*callback)(void *), void *args);
      kvs_result kv_pget(const Slice *key, char*& vbuf, int count, int offset);
      kvs_result kv_delete(const Slice *key);
      kvs_result kv_delete_async(const Slice *key, void (*callback)(void *), void *args);
      kvs_result kv_scan_keys(std::vector<std::string> &keys, int buf_size = 32768);

      int get_dev_util() {
        return dev_num_ == 1 ? dev_sd_->get_dev_util() : dev_md_->get_dev_util();
      }

      int64_t get_capacity() {
        return dev_num_ == 1 ? dev_sd_->get_capacity() : dev_md_->get_capacity();
      }

      double get_util() {
        return double(get_dev_util())/1000000;
      }


      class kv_iter {
      public:
        KVSSD_SD::kv_iter *iter_sd_;
        KVSSD_MD::kv_iter *iter_md_;
        int buf_size_;
        int dev_type_; // 1-single, else-multi-dev
      public:
        kv_iter(int buf_size = 32768); // current kvssd iter buffer size is fixed 32KB
        ~kv_iter() {
          if (iter_sd_) delete iter_sd_;
          if (iter_md_) delete iter_md_;
        };
        int get_num_entries (); 
        uint8_t* get_buffer ();
        
      };
      bool kv_iter_open(kv_iter *iter);
      bool kv_iter_next(kv_iter *iter); // true-continue, false-end
      bool kv_iter_close(kv_iter *iter);

  };
} // end namespace


#endif