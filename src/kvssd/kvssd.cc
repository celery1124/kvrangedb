#include "kvssd/kvssd.h"
#include "../hash.h"
#include <string>
#define ITER_BUFF 32768

namespace kvssd {

  typedef struct {
    sem_t *q_sem;
    void *args;
  } aio_context;

  void on_io_complete(kvs_callback_context* ioctx) {
    if (ioctx->result == KVS_ERR_CONT_NOT_EXIST) {
      int test = 1;
    }
    if (ioctx->result != 0 && ioctx->result != KVS_ERR_KEY_NOT_EXIST) {
      printf("io error: op = %d, key = %s, result = 0x%x, err = %s\n", ioctx->opcode, ioctx->key ? (char*)ioctx->key->key:0, ioctx->result, kvs_errstr(ioctx->result));
      exit(1);
    }

    aio_context *aio_ctx = (aio_context *)ioctx->private2;
    sem_post(aio_ctx->q_sem);
    
    switch (ioctx->opcode) {
    case IOCB_ASYNC_PUT_CMD : {
      void (*callback_put) (void *) = (void (*)(void *))ioctx->private1;
      void *args_put = (void *)aio_ctx->args;
      if (callback_put != NULL) {
        callback_put((void *)args_put);
      }
      if(ioctx->key) free(ioctx->key);
      if(ioctx->value) free(ioctx->value);
      break;
    }
    case IOCB_ASYNC_GET_CMD : {
      void (*callback_get) (void *) = (void (*)(void *))ioctx->private1;
      Async_sd_get_context *args_get = (Async_sd_get_context *)aio_ctx->args;
      KVSSD_SD *kvd = args_get->dev;
      args_get->vbuf = (char*) ioctx->value->value;
      args_get->actual_len = ioctx->value->actual_value_size;
      RecordTick(kvd->statistics, IO_GET_BYTES, args_get->actual_len);
      if (callback_get != NULL) {
        callback_get((void *)args_get->args);
      }
      delete args_get;
      if(ioctx->key) free(ioctx->key);
      if(ioctx->value) free(ioctx->value);
      break;
    }
    case IOCB_ASYNC_DEL_CMD : {
      void (*callback_del) (void *) = (void (*)(void *))ioctx->private1;
      void *args_del = (void *)aio_ctx->args;
      if (callback_del != NULL) {
        callback_del((void *)args_del);
      }
      if(ioctx->key) free(ioctx->key);
      break;
    }
    default : {
      printf("aio cmd error \n");
      break;
    }
    }

    return;
  }

  struct iterator_info{
    kvs_iterator_handle iter_handle;
    kvs_iterator_list iter_list;
    int has_iter_finish;
    kvs_iterator_option g_iter_mode;
  };

  bool KVSSD_SD::kv_exist (const Slice *key) {
    const kvs_key  kvskey = { (void *)key->data(), (uint8_t)key->size()};
    uint8_t result_buf[1];
    const kvs_exist_context exist_ctx = {NULL, NULL};
    kvs_exist_tuples(cont_handle, 1, &kvskey, 1, result_buf, &exist_ctx);
    //printf("[kv_exist] key: %s, existed: %d\n", std::string(key->data(),key->size()).c_str(), (int)result_buf[0]&0x1 == 1);
    return result_buf[0]&0x1 == 1;
  }

  uint32_t KVSSD_SD::kv_get_size(const Slice *key) {
    kvs_tuple_info info;

    const kvs_key  kvskey = { (void *)key->data(), (uint8_t)key->size()};
    kvs_result ret = kvs_get_tuple_info(cont_handle, &kvskey, &info);
    if (ret != KVS_SUCCESS) {
        printf("get info tuple failed with err %s\n", kvs_errstr(ret));
        exit(1);
    }
    //printf("[kv_get_size] key: %s, size: %d\n", std::string(key->data(),key->size()).c_str(), info.value_length);
    return info.value_length;
  }

  kvs_result KVSSD_SD::kv_store(const Slice *key, const Slice *val) {
    kvs_store_option option;
    option.st_type = KVS_STORE_POST;
    option.kvs_store_compress = false;

    const kvs_store_context put_ctx = {option, 0, 0};
    const kvs_key  kvskey = { (void *)key->data(), (uint8_t)key->size()};
    const kvs_value kvsvalue = { (void *)val->data(), val->size(), 0, 0 /*offset */};
    kvs_result ret = kvs_store_tuple(cont_handle, &kvskey, &kvsvalue, &put_ctx);

    if (ret != KVS_SUCCESS) {
        printf("STORE tuple failed with err %s, key %s\n", kvs_errstr(ret), std::string(key->data(), key->size()).c_str());
        exit(1);
    }

    RecordTick(statistics, IO_PUT);
    RecordTick(statistics, IO_PUT_BYTES, val->size());
    //printf("[kv_store] key: %s, size: %d\n",std::string(key->data(),key->size()).c_str(), val->size());
    return ret;
  }

  kvs_result KVSSD_SD::kv_store_async(Slice *key, Slice *val, void (*callback)(void *), void *args) {
    sem_wait(&q_sem);
    kvs_store_option option;
    option.st_type = KVS_STORE_POST;
    option.kvs_store_compress = false;

    aio_context *aio_ctx = new aio_context {&q_sem, args};
    const kvs_store_context put_ctx = {option, (void *)callback, (void *)aio_ctx};
    kvs_key *kvskey = (kvs_key*)malloc(sizeof(kvs_key));
    kvskey->key = (void *)key->data();
    kvskey->length = (uint8_t)key->size();
    kvs_value *kvsvalue = (kvs_value*)malloc(sizeof(kvs_value));
    kvsvalue->value = (void *)val->data();
    kvsvalue->length = val->size();
    kvsvalue->actual_value_size = kvsvalue->offset = 0;
    kvs_result ret = kvs_store_tuple_async(cont_handle, kvskey, kvsvalue, &put_ctx, on_io_complete);
    
    if (ret != KVS_SUCCESS) {
        printf("kv_store_async error %s\n", kvs_errstr(ret));
        exit(1);
    }

    RecordTick(statistics, IO_PUT);
    RecordTick(statistics, IO_PUT_BYTES, val->size());
    return ret;
  }
  // (not support in device)
  // kvs_result KVSSD_SD::kv_append(const Slice *key, const Slice *val) {
  //   kvs_store_option option;
  //   option.st_type = KVS_STORE_APPEND;
  //   option.kvs_store_compress = false;

  //   const kvs_store_context put_ctx = {option, 0, 0};
  //   const kvs_key  kvskey = { (void *)key->data(), (uint8_t)key->size()};
  //   const kvs_value kvsvalue = { (void *)val->data(), val->size(), 0, 0 /*offset */};
  //   kvs_result ret = kvs_store_tuple(cont_handle, &kvskey, &kvsvalue, &put_ctx);

  //   if (ret != KVS_SUCCESS) {
  //       printf("APPEND tuple failed with err %s\n", kvs_errstr(ret));
  //       exit(1);
  //   }
  //   //printf("[kv_append] key: %s, size: %d\n",std::string(key->data(),key->size()).c_str(), val->size());
  //   return ret;
  // }

  // inplement append using kv_store and kv_get
  kvs_result KVSSD_SD::kv_append(const Slice *key, const Slice *val) {
    // get old KV
    char *vbuf; int vlen;
    kvs_result ret;
    {
      vbuf = (char *) malloc(INIT_GET_BUFF);
      const kvs_key  kvskey = { (void *)key->data(), (uint8_t)key->size() };
      kvs_value kvsvalue = { vbuf, INIT_GET_BUFF , 0, 0 /*offset */}; //prepare initial buffer
      kvs_retrieve_option option;
      memset(&option, 0, sizeof(kvs_retrieve_option));
      option.kvs_retrieve_decompress = false;
      option.kvs_retrieve_delete = false;
      const kvs_retrieve_context ret_ctx = {option, 0, 0};
      ret = kvs_retrieve_tuple(cont_handle, &kvskey, &kvsvalue, &ret_ctx);
      if(ret != KVS_ERR_KEY_NOT_EXIST) {
        vlen = kvsvalue.actual_value_size;
        if (INIT_GET_BUFF < vlen) {
          // implement own aligned_realloc
          char *realloc_vbuf = (char *) malloc(vlen + 4 - (vlen%4));
          memcpy(realloc_vbuf, vbuf, INIT_GET_BUFF);
          free(vbuf); vbuf = realloc_vbuf;
          kvsvalue.value = vbuf;
          kvsvalue.length = vlen + 4 - (vlen%4);
          kvsvalue.offset = INIT_GET_BUFF; // skip the first IO buffer (not support, actually read whole value)
          ret = kvs_retrieve_tuple(cont_handle, &kvskey, &kvsvalue, &ret_ctx);
        }
      }

    }
    

    kvs_store_option option;
    option.st_type = KVS_STORE_POST;
    option.kvs_store_compress = false;

    const kvs_store_context put_ctx = {option, 0, 0};
    const kvs_key  kvskey = { (void *)key->data(), (uint8_t)key->size()};
    kvs_value kvsvalue;
    if (ret == KVS_SUCCESS) { // key exist, append
      vbuf = (char *)realloc(vbuf, vlen+val->size());
      memcpy(vbuf+vlen, val->data(), val->size());
      kvsvalue = { vbuf, vlen+val->size(), 0, 0 /*offset */};
    }
    else { // key not exist, store
      kvsvalue = { (void *)val->data(), val->size(), 0, 0 /*offset */};
    }
    
    ret = kvs_store_tuple(cont_handle, &kvskey, &kvsvalue, &put_ctx);

    if (ret != KVS_SUCCESS) {
        printf("APPEND tuple failed with err %s\n", kvs_errstr(ret));
        exit(1);
    }
    free(vbuf); // release buffer from kv_get

    RecordTick(statistics, IO_APPEND);
    RecordTick(statistics, IO_APP_BYTES, val->size());
    //printf("[kv_append] key: %s, size: %d\n",std::string(key->data(),key->size()).c_str(), val->size());
    return ret;
  }


  typedef struct {
    KVSSD_SD *kvd;
    Slice *key;
    Slice *val;
    char *vbuf;
    uint32_t vbuf_size;
    void (*cb) (void *) ;
    void *args;
  } Async_append_context;

  typedef struct {
    char *vbuf;
    void (*cb) (void *) ;
    void *args;
    Async_append_context *append_ctx;
  } Async_append_cleanup;

  static void kv_append_cleanup (void *args) {
    Async_append_cleanup *cleanup = (Async_append_cleanup *)args;
    free(cleanup->vbuf);
    if (cleanup->cb != NULL)
      cleanup->cb(cleanup->args);

    free ((void *)cleanup->append_ctx->key->data());
    free ((void *)cleanup->append_ctx->val->data());
    delete cleanup->append_ctx->key;
    delete cleanup->append_ctx->val;
    delete cleanup->append_ctx;
    delete cleanup;
  }

  static void kv_append_async_callback(void *args) {
    // store new value
    Async_append_context *append_ctx = (Async_append_context *)args;
    
    // append value
    append_ctx->vbuf = (char *)realloc(append_ctx->vbuf, append_ctx->vbuf_size+append_ctx->val->size());  
    memcpy(append_ctx->vbuf+append_ctx->vbuf_size, append_ctx->val->data(), append_ctx->val->size());
    Slice new_val (append_ctx->vbuf, append_ctx->val->size() + append_ctx->vbuf_size);

    Async_append_cleanup *cleanup = new Async_append_cleanup;
    cleanup->vbuf = append_ctx->vbuf;
    cleanup->cb = append_ctx->cb;
    cleanup->args = append_ctx->args;
    cleanup->append_ctx = append_ctx;
    append_ctx->kvd->kv_store_async(append_ctx->key, &new_val, 
                              kv_append_cleanup, cleanup);
    
  }

  // implement async kv append using kv_get_async and kv_store_async
  kvs_result KVSSD_SD::kv_append_async(const Slice *key, const Slice *val, void (*callback)(void *), void *args) {
    // get old KV
    Async_append_context *io_ctx = new Async_append_context;
    io_ctx->kvd = this;
    io_ctx->key = (Slice *)key;
    io_ctx->val = (Slice *)val;
    io_ctx->vbuf = NULL;
    io_ctx->vbuf_size = 0;
    io_ctx->cb = callback;
    io_ctx->args = args;

    Async_sd_get_context *get_ctx = new Async_sd_get_context(this, io_ctx->vbuf, io_ctx->vbuf_size, io_ctx);

    return kv_get_async(key, kv_append_async_callback, get_ctx);

    RecordTick(statistics, IO_APPEND);
    RecordTick(statistics, IO_APP_BYTES, val->size());
  }

  kvs_result KVSSD_SD::kv_get_oneshot(const Slice *key, char* vbuf, int vlen) {
    // vbuf already allocated, read in 1 I/O
    const kvs_key  kvskey = { (void *)key->data(), (uint8_t)key->size() };
    kvs_value kvsvalue = { vbuf, vlen , 0, 0 /*offset */}; //prepare initial buffer
    kvs_retrieve_option option;
    memset(&option, 0, sizeof(kvs_retrieve_option));
    option.kvs_retrieve_decompress = false;
    option.kvs_retrieve_delete = false;
    const kvs_retrieve_context ret_ctx = {option, 0, 0};
    kvs_result ret = kvs_retrieve_tuple(cont_handle, &kvskey, &kvsvalue, &ret_ctx);
    //printf("[kv_get] key: %s, size: %d\n",std::string(key->data(),key->size()).c_str(), vlen);
    assert(vlen == kvsvalue.actual_value_size);
    return ret;
  }

  kvs_result KVSSD_SD::kv_get(const Slice *key, char*& vbuf, int& vlen, int init_size /* default = INIT_GET_BUFF */) {
    RecordTick(statistics, IO_GET);
    vbuf = (char *) malloc(init_size);
    const kvs_key  kvskey = { (void *)key->data(), (uint8_t)key->size() };
    kvs_value kvsvalue = { vbuf, init_size , 0, 0 /*offset */}; //prepare initial buffer
    kvs_retrieve_option option;
    memset(&option, 0, sizeof(kvs_retrieve_option));
    option.kvs_retrieve_decompress = false;
    option.kvs_retrieve_delete = false;
    const kvs_retrieve_context ret_ctx = {option, 0, 0};
    kvs_result ret = kvs_retrieve_tuple(cont_handle, &kvskey, &kvsvalue, &ret_ctx);
    if(ret == KVS_ERR_KEY_NOT_EXIST) {
      RecordTick(statistics, IO_GET_NONE);
      return ret;
    }
    //if (ret == KVS_ERR_BUFFER_SMALL) { // do anther IO KVS_ERR_BUFFER_SMALL not working
    vlen = kvsvalue.actual_value_size;
    RecordTick(statistics, IO_GET_BYTES, vlen);
    if (init_size < vlen) {
      // implement own aligned_realloc
      int realloc_buf_size = (vlen + 4 - (vlen%4)) > (2<<20) ? (2<<20) : (vlen + 4 - (vlen%4));
      char *realloc_vbuf = (char *) malloc(realloc_buf_size);
      memcpy(realloc_vbuf, vbuf, init_size);
      free(vbuf); vbuf = realloc_vbuf;
      kvsvalue.value = vbuf;
      kvsvalue.length = realloc_buf_size;
      kvsvalue.offset = init_size; // skip the first IO buffer (not support, actually read whole value)
      ret = kvs_retrieve_tuple(cont_handle, &kvskey, &kvsvalue, &ret_ctx);

      RecordTick(statistics, IO_GET);
      RecordTick(statistics, IO_GET_BYTES, realloc_buf_size);
    }
    //printf("[kv_get] key: %s, size: %d\n",std::string(key->data(),key->size()).c_str(), vlen);
    return ret;
  }
  // ***** limitations *****
  // currently consider async get buffer size is large enough
  // in other words, async get can retrieve the whole value with 1 I/O.
  kvs_result KVSSD_SD::kv_get_async(const Slice *key, void (*callback)(void *), void *args) {
    sem_wait(&q_sem);
    char *vbuf = (char *) malloc(INIT_GET_BUFF);
    kvs_key *kvskey = (kvs_key*)malloc(sizeof(kvs_key));
    kvskey->key = (void *) key->data();
    kvskey->length = key->size();
    kvs_value *kvsvalue = (kvs_value*)malloc(sizeof(kvs_value));
    kvsvalue->value = vbuf;
    kvsvalue->length = INIT_GET_BUFF;
    kvsvalue->actual_value_size = kvsvalue->offset = 0;
  
    kvs_retrieve_option option;
    memset(&option, 0, sizeof(kvs_retrieve_option));
    option.kvs_retrieve_decompress = false;
    option.kvs_retrieve_delete = false;

    aio_context *aio_ctx = new aio_context {&q_sem, args};
    const kvs_retrieve_context ret_ctx = {option, (void *)callback, (void*)aio_ctx};
    kvs_result ret = kvs_retrieve_tuple_async(cont_handle, kvskey, kvsvalue, &ret_ctx, on_io_complete);
    if(ret != KVS_SUCCESS) {
      printf("kv_get_async error %d\n", ret);
      exit(1);
    }
    RecordTick(statistics, IO_GET);
    return KVS_SUCCESS;
  }

  // offset must be 64byte aligned (not support)
  kvs_result KVSSD_SD::kv_pget(const Slice *key, char*& vbuf, int count, int offset) {
    vbuf = (char *) malloc(count+64);
    const kvs_key  kvskey = { (void *)key->data(), (uint8_t)key->size() };
    kvs_value kvsvalue = { vbuf, count , 0, offset /*offset */}; 
    kvs_retrieve_option option;
    memset(&option, 0, sizeof(kvs_retrieve_option));
    option.kvs_retrieve_decompress = false;
    option.kvs_retrieve_delete = false;
    const kvs_retrieve_context ret_ctx = {option, 0, 0};
    kvs_result ret = kvs_retrieve_tuple(cont_handle, &kvskey, &kvsvalue, &ret_ctx);
    if(ret != KVS_SUCCESS) {
      printf("position get tuple failed with error %s\n", kvs_errstr(ret));
      exit(1);
    }
    //printf("[kv_pget] key: %s, count: %d, offset: %d\n",std::string(key->data(),key->size()).c_str(), count, offset);
    return ret;
  }

  kvs_result KVSSD_SD::kv_delete(const Slice *key) {
    const kvs_key  kvskey = { (void *)key->data(), (uint8_t)key->size() };
    const kvs_delete_context del_ctx = { {false}, 0, 0};
    kvs_result ret = kvs_delete_tuple(cont_handle, &kvskey, &del_ctx);

    if(ret != KVS_SUCCESS) {
        printf("delete tuple failed with error %s\n", kvs_errstr(ret));
        exit(1);
    }
    RecordTick(statistics, IO_DEL);
    //printf("[kv_delete] key: %s\n",std::string(key->data(),key->size()).c_str());
    return ret;
  }

  kvs_result KVSSD_SD::kv_delete_async(const Slice *key, void (*callback)(void *), void *args) {
    sem_wait(&q_sem);
    kvs_key *kvskey = (kvs_key*)malloc(sizeof(kvs_key));
    kvskey->key = (void *)key->data();
    kvskey->length = (uint8_t)key->size();

    aio_context *aio_ctx = new aio_context {&q_sem, args};
    const kvs_delete_context del_ctx = { {false}, (void *)callback, (void *)aio_ctx};
    kvs_result ret = kvs_delete_tuple_async(cont_handle, kvskey, &del_ctx, on_io_complete);

    if(ret != KVS_SUCCESS) {
        printf("kv_delete_async error %s\n", kvs_errstr(ret));
        exit(1);
    }
    RecordTick(statistics, IO_DEL);
    return ret;
  }

  kvs_result KVSSD_SD::kv_scan_keys(std::vector<std::string>& keys, int buf_size) {
    struct iterator_info *iter_info = (struct iterator_info *)malloc(sizeof(struct iterator_info));
    iter_info->g_iter_mode.iter_type = KVS_ITERATOR_KEY;
    
    int ret;
    //printf("start scan keys\n");
    /* Open iterator */

    kvs_iterator_context iter_ctx_open;
    iter_ctx_open.option = iter_info->g_iter_mode;
    iter_ctx_open.bitmask = 0x00000000;

    iter_ctx_open.bit_pattern = 0x00000000;
    iter_ctx_open.private1 = NULL;
    iter_ctx_open.private2 = NULL;
    
    ret = kvs_open_iterator(cont_handle, &iter_ctx_open, &iter_info->iter_handle);
    if(ret != KVS_SUCCESS) {
      printf("iterator open fails with error 0x%x - %s\n", ret, kvs_errstr(ret));
      free(iter_info);
      exit(1);
    }
      
    /* Do iteration */
    iter_info->iter_list.size = buf_size;
    uint8_t *buffer;
    buffer =(uint8_t*) kvs_malloc(buf_size, 4096);
    iter_info->iter_list.it_list = (uint8_t*) buffer;

    kvs_iterator_context iter_ctx_next;
    iter_ctx_next.option = iter_info->g_iter_mode;
    iter_ctx_next.private1 = iter_info;
    iter_ctx_next.private2 = NULL;

    while(1) {
      iter_info->iter_list.size = buf_size;
      memset(iter_info->iter_list.it_list, 0, buf_size);
      ret = kvs_iterator_next(cont_handle, iter_info->iter_handle, &iter_info->iter_list, &iter_ctx_next);
      if(ret != KVS_SUCCESS) {
        printf("iterator next fails with error 0x%x - %s\n", ret, kvs_errstr(ret));
        free(iter_info);
        exit(1);
      }
          
      uint8_t *it_buffer = (uint8_t *) iter_info->iter_list.it_list;
      uint32_t key_size = 0;
      
      for(int i = 0;i < iter_info->iter_list.num_entries; i++) {
        // get key size
        key_size = *((unsigned int*)it_buffer);
        it_buffer += sizeof(unsigned int);

        // add key
        keys.push_back(std::string((char *)it_buffer, key_size));
        it_buffer += key_size;
      }
          
      if(iter_info->iter_list.end) {
        break;
      } 
    }

    /* Close iterator */
    kvs_iterator_context iter_ctx_close;
    iter_ctx_close.private1 = NULL;
    iter_ctx_close.private2 = NULL;

    ret = kvs_close_iterator(cont_handle, iter_info->iter_handle, &iter_ctx_close);
    if(ret != KVS_SUCCESS) {
      printf("Failed to close iterator\n");
      exit(1);
    }
    
    if(buffer) kvs_free(buffer);
    if(iter_info) free(iter_info);
    return KVS_SUCCESS;
  }


  bool KVSSD_SD::kv_iter_open(kv_iter *iter) {
    /* Open iterator */
    kvs_iterator_context iter_ctx_open;
    iter_ctx_open.option = iter->iter_info->g_iter_mode;
    iter_ctx_open.bitmask = 0x00000000;

    iter_ctx_open.bit_pattern = 0x00000000;
    iter_ctx_open.private1 = NULL;
    iter_ctx_open.private2 = NULL;

    int ret = kvs_open_iterator(cont_handle, &iter_ctx_open, &iter->iter_info->iter_handle);
    if(ret != KVS_SUCCESS) {
      printf("iterator open fails with error 0x%x - %s\n", ret, kvs_errstr(ret));
      return false;
    }
    else return true;
      
  }

  int KVSSD_SD::kv_iter::get_num_entries () {return iter_info->iter_list.num_entries;}

  KVSSD_SD::kv_iter::kv_iter (int buf_size):buf_size_(buf_size)  {
    iter_info = (struct iterator_info *)malloc(sizeof(struct iterator_info));
    iter_info->g_iter_mode.iter_type = KVS_ITERATOR_KEY;
    iter_info->iter_list.size = buf_size;
    buffer_ =(uint8_t*) kvs_malloc(buf_size, 4096);
    iter_info->iter_list.it_list = (uint8_t*) buffer_;
  }

  bool KVSSD_SD::kv_iter_next(kv_iter *iter) {
    kvs_iterator_context iter_ctx_next;
    iter_ctx_next.option = iter->iter_info->g_iter_mode;
    iter_ctx_next.private1 = iter->iter_info;
    iter_ctx_next.private2 = NULL;

    iter->iter_info->iter_list.size = iter->buf_size_;
    memset(iter->iter_info->iter_list.it_list, 0, iter->buf_size_);
    int ret = kvs_iterator_next(cont_handle, iter->iter_info->iter_handle, &iter->iter_info->iter_list, &iter_ctx_next);

    if(ret != KVS_SUCCESS) { // we don't assume iterator next error here.
      printf("iterator next fails with error 0x%x - %s\n", ret, kvs_errstr(ret));
      exit(1);
    }
    else {
          
      if(iter->iter_info->iter_list.end) {
        return false; // end of iteration
      } 
      else return true;
    }
  }

  bool KVSSD_SD::kv_iter_close(kv_iter *iter) {
    /* Close iterator */
    kvs_iterator_context iter_ctx_close;
    iter_ctx_close.private1 = NULL;
    iter_ctx_close.private2 = NULL;

    int ret = kvs_close_iterator(cont_handle, iter->iter_info->iter_handle, &iter_ctx_close);
    if(ret != KVS_SUCCESS) {
      if(iter->iter_info) free(iter->iter_info);
      return false;
    }
    
    if(iter->iter_info) free(iter->iter_info);
    return true;
  }

  // KVMD

  uint64_t key_hash (const Slice *key) {
    return NPHash64(key->data(), key->size());
  }

  bool KVSSD_MD::kv_exist (const Slice *key) {
    int dev_id = key_hash(key)%dev_num_;
    return dev_list_[dev_id]->kv_exist(key);
  }

  uint32_t KVSSD_MD::kv_get_size(const Slice *key) {
    int dev_id = key_hash(key)%dev_num_;
    return dev_list_[dev_id]->kv_get_size(key);
  }

  kvs_result KVSSD_MD::kv_store(const Slice *key, const Slice *val) {
    int dev_id = key_hash(key)%dev_num_;
    return dev_list_[dev_id]->kv_store(key, val);
  }

  kvs_result KVSSD_MD::kv_store_async(Slice *key, Slice *val, void (*callback)(void *), void *args) {
    int dev_id = key_hash(key)%dev_num_;
    return dev_list_[dev_id]->kv_store_async(key, val, callback, args);
  }

  kvs_result KVSSD_MD::kv_append(const Slice *key, const Slice *val) {
    int dev_id = key_hash(key)%dev_num_;
    return dev_list_[dev_id]->kv_append(key, val);
  }

  kvs_result KVSSD_MD::kv_append_async(const Slice *key, const Slice *val, void (*callback)(void *), void *args) {
    int dev_id = key_hash(key)%dev_num_;
    return dev_list_[dev_id]->kv_append_async(key, val, callback, args);
  }

  kvs_result KVSSD_MD::kv_get_oneshot(const Slice *key, char* vbuf, int vlen) {
    int dev_id = key_hash(key)%dev_num_;
    return dev_list_[dev_id]->kv_get_oneshot(key, vbuf, vlen);
  }

  kvs_result KVSSD_MD::kv_get(const Slice *key, char*& vbuf, int& vlen, int init_size /* default = INIT_GET_BUFF */) {
    int dev_id = key_hash(key)%dev_num_;
    return dev_list_[dev_id]->kv_get(key, vbuf, vlen, init_size);
  }
  // ***** limitations *****
  // currently consider async get buffer size is large enough
  // in other words, async get can retrieve the whole value with 1 I/O.
  kvs_result KVSSD_MD::kv_get_async(const Slice *key, void (*callback)(void *), void *args) {
    int dev_id = key_hash(key)%dev_num_;
    return dev_list_[dev_id]->kv_get_async(key, callback, args);
  }

  // offset must be 64byte aligned (not support)
  kvs_result KVSSD_MD::kv_pget(const Slice *key, char*& vbuf, int count, int offset) {
    int dev_id = key_hash(key)%dev_num_;
    return dev_list_[dev_id]->kv_pget(key, vbuf, count, offset);
  }

  kvs_result KVSSD_MD::kv_delete(const Slice *key) {
    int dev_id = key_hash(key)%dev_num_;
    return dev_list_[dev_id]->kv_delete(key);
  }

  kvs_result KVSSD_MD::kv_delete_async(const Slice *key, void (*callback)(void *), void *args) {
    int dev_id = key_hash(key)%dev_num_;
    return dev_list_[dev_id]->kv_delete_async(key, callback, args);
  }

  kvs_result KVSSD_MD::kv_scan_keys(std::vector<std::string>& keys, int buf_size) {
    /* open iterator on each device */
    KVSSD_SD::kv_iter **iters = new KVSSD_SD::kv_iter*[dev_num_];
    for (int i = 0; i < dev_num_; i++) {
      iters[i] = new KVSSD_SD::kv_iter(buf_size);
      if (dev_list_[i]->kv_iter_open(iters[i]) == false) {
        exit(-1); // simply exit program
      }
    }

      
    /* Do iteration on each device one by one */

    for (int i = 0; i < dev_num_; i++) {
      while(1) {
        bool iter_cont = dev_list_[i]->kv_iter_next(iters[i]);
        uint8_t *it_buffer = iters[i]->buffer_;
        uint32_t key_size = 0;
          int iter_num_entries = iters[i]->get_num_entries();
          for(int i = 0;i < iter_num_entries; i++) {
            // get key size
            key_size = *((unsigned int*)it_buffer);
            it_buffer += sizeof(unsigned int);

            keys.push_back(std::string((char*)it_buffer, key_size));
            
            it_buffer += key_size;
          }
          if (!iter_cont) break; // finish iteration
      }
    }

    /* Close iterator */
    for (int i = 0; i < dev_num_; i++) {
      if (dev_list_[i]->kv_iter_close(iters[i]) == false) {
        exit(-1); // simply exit program
      }
      delete iters[i];
    }
    delete iters;
    return KVS_SUCCESS;
  }


  bool KVSSD_MD::kv_iter_open(kv_iter *iter) {
    /* Open iterator */
    iter->iters_ = new KVSSD_SD::kv_iter*[dev_num_];
    for (int i = 0; i < dev_num_; i++) {
      iter->iters_[i] = new KVSSD_SD::kv_iter(iter->buf_size_);
      dev_list_[i]->kv_iter_open(iter->iters_[i]);
    }
    // allocate another buffer to contain internal buffer for each dev iterator
    iter->buffer_ = (uint8_t*) kvs_malloc(iter->buf_size_*dev_num_, 4096);
  }

  int KVSSD_MD::kv_iter::get_num_entries () {
    return buf_entries_;
  }

  KVSSD_MD::kv_iter::kv_iter (int buf_size):buf_size_(buf_size)  {}

  bool KVSSD_MD::kv_iter_next(kv_iter *iter) {
    iter->buf_entries_ = 0;
    uint8_t *buffer = iter->buffer_;
    for (int i = 0; i < dev_num_; i++) {
      if (dev_list_[i]->kv_iter_next(iter->iters_[i]) == false) {
        return false;
      }

      // copy buffers
      memcpy(buffer, iter->iters_[i]->buffer_, iter->buf_size_);
      buffer += iter->buf_size_;
      iter->buf_entries_ += iter->get_num_entries();
    }
    return true;
  }

  bool KVSSD_MD::kv_iter_close(kv_iter *iter) {
    /* Close iterator */
    for (int i = 0; i < dev_num_; i++) {
      dev_list_[i]->kv_iter_close(iter->iters_[i]);
      delete iter->iters_[i];
    }
    delete iter->iters_;
    return true;
  }

  // KVSSD

  bool KVSSD::kv_exist (const Slice *key) {
    return dev_num_ == 1 ? dev_sd_->kv_exist(key) : dev_md_->kv_exist(key);
  }

  uint32_t KVSSD::kv_get_size(const Slice *key) {
    return dev_num_ == 1 ? dev_sd_->kv_get_size(key) : dev_md_->kv_get_size(key);
  }

  kvs_result KVSSD::kv_store(const Slice *key, const Slice *val) {
    return dev_num_ == 1 ? dev_sd_->kv_store(key, val) : dev_md_->kv_store(key, val);
  }

  kvs_result KVSSD::kv_store_async(Slice *key, Slice *val, void (*callback)(void *), void *args) {
    
    return dev_num_ == 1 ? dev_sd_->kv_store_async(key, val, callback, args) : dev_md_->kv_store_async(key, val, callback, args);
  }

  kvs_result KVSSD::kv_append(const Slice *key, const Slice *val) {
    return dev_num_ == 1 ? dev_sd_->kv_append(key, val) : dev_md_->kv_append(key, val);
  }

  kvs_result KVSSD::kv_append_async(const Slice *key, const Slice *val, void (*callback)(void *), void *args) {
    return dev_num_ == 1 ? dev_sd_->kv_append_async(key, val, callback, args) : dev_md_->kv_append_async(key, val, callback, args);
  }

  kvs_result KVSSD::kv_get_oneshot(const Slice *key, char* vbuf, int vlen) {
    return dev_num_ == 1 ? dev_sd_->kv_get_oneshot(key, vbuf, vlen) : dev_md_->kv_get_oneshot(key, vbuf, vlen);
  }

  kvs_result KVSSD::kv_get(const Slice *key, char*& vbuf, int& vlen, int init_size /* default = INIT_GET_BUFF */) {
    return dev_num_ == 1 ? dev_sd_->kv_get(key, vbuf, vlen, init_size) : dev_md_->kv_get(key, vbuf, vlen, init_size);
  }
  // ***** limitations *****
  // currently consider async get buffer size is large enough
  // in other words, async get can retrieve the whole value with 1 I/O.
  kvs_result KVSSD::kv_get_async(const Slice *key, void (*callback)(void *), void *args) {
    return dev_num_ == 1 ? dev_sd_->kv_get_async(key, callback, args) : dev_md_->kv_get_async(key, callback, args);
  }

  // offset must be 64byte aligned (not support)
  kvs_result KVSSD::kv_pget(const Slice *key, char*& vbuf, int count, int offset) {
    return dev_num_ == 1 ? dev_sd_->kv_pget(key, vbuf, count, offset) : dev_md_->kv_pget(key, vbuf, count, offset);
  }

  kvs_result KVSSD::kv_delete(const Slice *key) {
    return dev_num_ == 1 ? dev_sd_->kv_delete(key) : dev_md_->kv_delete(key);
  }

  kvs_result KVSSD::kv_delete_async(const Slice *key, void (*callback)(void *), void *args) {
    return dev_num_ == 1 ? dev_sd_->kv_delete_async(key, callback, args) : dev_md_->kv_delete_async(key, callback, args);
  }

  kvs_result KVSSD::kv_scan_keys(std::vector<std::string>& keys, int buf_size) {
    return dev_num_ == 1 ? dev_sd_->kv_scan_keys(keys, buf_size) : dev_md_->kv_scan_keys(keys, buf_size);
  }


  bool KVSSD::kv_iter_open(kv_iter *iter) {
    iter->dev_type_ = dev_num_ == 1 ? 1 : dev_num_;
    if (iter->dev_type_ == 1) {
      iter->iter_sd_ = new KVSSD_SD::kv_iter(iter->buf_size_);
      iter->iter_md_ = NULL;
    }
    else {
      iter->iter_sd_ = NULL;
      iter->iter_md_ = new KVSSD_MD::kv_iter(iter->buf_size_);
    }
  }

  int KVSSD::kv_iter::get_num_entries () {
    return dev_type_ == 1 ? iter_sd_->get_num_entries() : iter_md_->get_num_entries();
  }
  uint8_t* KVSSD::kv_iter::get_buffer () {
    return dev_type_ == 1 ? iter_sd_->buffer_ : iter_md_->buffer_;
  }

  KVSSD::kv_iter::kv_iter (int buf_size):buf_size_(buf_size)  {}

  bool KVSSD::kv_iter_next(kv_iter *iter) {
    if(iter->dev_type_ == 1) dev_sd_->kv_iter_next(iter->iter_sd_);
    else dev_md_->kv_iter_next(iter->iter_md_);
    return true;
  }

  bool KVSSD::kv_iter_close(kv_iter *iter) {
    if(iter->dev_type_ == 1) dev_sd_->kv_iter_close(iter->iter_sd_);
    else dev_md_->kv_iter_close(iter->iter_md_);
    return true;
  }
} // end namespace
