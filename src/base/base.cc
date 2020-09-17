/* base.cc
* 08/05/2019
* by Mian Qin
*/

#include "base.h"
#include <string>

namespace base {

BaseOrder::Iterator::Iterator (BaseOrder *base, int scan_len) 
  : scan_len_(scan_len), base_(base), ordered_keys_(custom_cmp(base->cmp_)) {
  // sanctity check
  if (scan_len_ <= 0) scan_len_ = 1;
  KVIter_ = new kvssd::KVSSD::kv_iter();
  base_->kvd_->kv_iter_open(KVIter_);
}

BaseOrder::Iterator::~Iterator () {
  base_->kvd_->kv_iter_close(KVIter_);
  delete KVIter_;
}

void BaseOrder::Iterator::SeekToFirst() {
  Slice key_target(NULL, 0);
  Seek(&key_target);
}

void BaseOrder::Iterator::Seek(Slice *key) {
  while (true) {
    bool iter_cont = base_->kvd_->kv_iter_next(KVIter_);
    uint8_t *it_buffer = KVIter_->buffer_;
    uint32_t key_size = 0;
      int iter_num_entries = KVIter_->get_num_entries();
      for(int i = 0;i < iter_num_entries; i++) {
        // get key size
        key_size = *((unsigned int*)it_buffer);
        it_buffer += sizeof(unsigned int);

        std::string curr_key_str((char*)it_buffer, key_size);
        Slice curr_key(curr_key_str);
        // filter out keys not in range
        if (base_->cmp_->Compare(curr_key, *key) >= 0) {
          ordered_keys_.insert(curr_key_str);
          if (ordered_keys_.size() > scan_len_) { //remove last 
            auto last_it = --ordered_keys_.end();
            ordered_keys_.erase(last_it);
          }
        }
        
        it_buffer += key_size;
      }
      if (!iter_cont) break; // finish iteration
  }
  it_ = ordered_keys_.begin();
}

void BaseOrder::Iterator::Next() {
  ++it_;
}

bool BaseOrder::Iterator::Valid() {
  return it_ != ordered_keys_.end();
}

Slice BaseOrder::Iterator::key() {
    return *it_;
}

BaseOrder::Iterator* BaseOrder::NewIterator(int scan_len) {
    return new Iterator(this, scan_len);
}

} // end namespace base
