/* base.h
* 08/05/2019
* by Mian Qin
*/

#ifndef _base_h
#define _base_h

#include <stdlib.h>
#include <set>

#include "slice.h"
#include "comparator.h"
#include "kvssd/kvssd.h"

namespace base {

struct custom_cmp {
  explicit custom_cmp(Comparator *cmp) : cmp_(cmp) {}
  bool operator() (Slice l, Slice r) const {
    return (cmp_->Compare(l, r) < 0) ;
  }
  Comparator *cmp_;
};

class BaseOrder {
public:
  BaseOrder(Comparator *cmp, kvssd::KVSSD *kvd) : cmp_(cmp), kvd_(kvd) {};
  ~BaseOrder() {}
  kvssd::KVSSD* GetDev() {return kvd_;}

  class Iterator {
  private: 
    int scan_len_;
    kvssd::KVSSD::kv_iter *KVIter_;
    BaseOrder *base_;
    std::vector<std::string> keys_;
    std::set<Slice, custom_cmp> ordered_keys_;
    std::set<Slice, custom_cmp>::iterator it_;
  public:
    Iterator (BaseOrder *base, int scan_len);
    ~Iterator () ;
    void Seek(Slice *key);
    void SeekToFirst();
    void Next();
    bool Valid();
    Slice key();
  };
  Iterator* NewIterator(int scan_len);

private:
    Comparator *cmp_;
    kvssd::KVSSD* kvd_;
};

} // end namespace base


#endif