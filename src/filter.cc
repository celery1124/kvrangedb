// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include "filter.h"
#include <string>

namespace kvrangedb {

class HiBloomFilter : public RangeFilter {
 public:

  HiBloomFilter(int bits_per_key, int bits_per_level, int levels, int exam_suffix_bits, int num_keys, int max_probes, Statistics *stats) 
  : num_keys_(num_keys), max_probes_(max_probes), bits_per_level_(bits_per_level), levels_(levels), total_bits_per_key_(bits_per_key), statistics_(stats) {
    
    int filter_bytes = 0;
    bits_per_key_ = new double[levels];
    k_ = new size_t[levels];
    bits_ = new size_t[levels];
    bf_ = new std::string[levels];
    // total # of suffix bits examined using filter, larger require more probes
    range_dist_bits_thres_ = exam_suffix_bits;
    range_dist_bits_thres_ = range_dist_bits_thres_>30 ? 30 : range_dist_bits_thres_; // hard limit

    for (int i = 0; i < levels; i++) {
      // bits_per_key_[i] = (double)bits_per_key / levels;
      // bits_per_key_[i] = (double)bits_per_key * (i+1) / ((levels+1)*levels/2);
      bits_per_key_[i] = (double)bits_per_key * (levels-i) / ((levels+1)*levels/2);
      // We intentionally round down to reduce probing cost a little bit
      k_[i] = static_cast<size_t>(bits_per_key_[i] * 0.69);  // 0.69 =~ ln(2)
      if (k_[i] < 1) k_[i] = 1;
      if (k_[i] > 30) k_[i] = 30;
      bits_[i] = static_cast<size_t>(num_keys * bits_per_key_[i]);
      if (bits_[i] < 64) bits_[i] = 64;
      bits_[i] = (bits_[i] + 7) / 8 * 8;

      // filter data
      bf_[i].resize(bits_[i]/8, 0);
      bf_[i].push_back(static_cast<char>(k_[i]));
      filter_bytes += (bits_[i]/8+1);
    }
    printf("HiBloomFilter (%d levels, %d bits per level) #keys: %d size: %.3f MB\n",levels, bits_per_level, num_keys, (double)filter_bytes/1024/1024);
  }

  ~HiBloomFilter() {
    delete [] bits_per_key_;
    delete [] k_;
    delete [] bits_;
    delete [] bf_;
  }

  virtual std::string GenFilterName() {
    return "/tmp/HiBloomFilter_"+std::to_string(num_keys_)+"_"+std::to_string(total_bits_per_key_)+"_"+std::to_string(levels_);
  }

  void LoadFilter(std::string filename) {
    for (int i = 0; i < levels_; i++) {
      std::ifstream filterFile;
      std::string fn = filename+"_"+std::to_string(i);
      filterFile.open (fn.c_str(), std::ios::in | std::ios::binary);
      filterFile.read ((char *)bf_[i].data(), bf_[i].size());

      filterFile.close();
    }
  }

  void SaveFilter(std::string filename) {
    std::ofstream filterFile;
    filterFile.open (filename.c_str(), std::ios::out | std::ios::binary);
    filterFile << "";
    filterFile.close();
    for (int i = 0; i < levels_; i++) {
      std::ofstream filterFile;
      std::string fn = filename+"_"+std::to_string(i);
      filterFile.open (fn.c_str(), std::ios::out | std::ios::binary);
      filterFile << bf_[i];
      filterFile.close();
    }
  }
  
  void InsertItem(const Slice& key)  {
    std::string prefix_key = key.ToString();
    std::reverse(prefix_key.begin(), prefix_key.end());
    size_t klen = key.size();
    for (int l = 0; l < levels_; l++) {
      // mask suffix for each level
      uint32_t *suffix = (uint32_t *)&prefix_key[0];
      *suffix = *suffix >> (l*bits_per_level_);
      *suffix = *suffix << (l*bits_per_level_);
      
      char* array = &(bf_[l][0]);
      // Use double-hashing to generate a sequence of hash values.
      // See analysis in [Kirsch,Mitzenmacher 2006].
      uint64_t h = BloomHash(prefix_key);
      const uint64_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
      for (size_t j = 0; j < k_[l]; j++) {
        const uint64_t bitpos = h % bits_[l];
        array[bitpos / 8] |= (1 << (bitpos % 8));
        h += delta;
      }
    }
  }

  bool KeyMayMatch(const Slice& key) {
    RecordTick(statistics_, FILTER_POINT_CHECK);
    std::string le_key = key.ToString();
    std::reverse(le_key.begin(), le_key.end());

    return KeyMayMatchLevel(le_key, 0); // last level BF
  }

  bool RangeMayMatch(const Slice& lkey, const Slice& hkey) {
    RecordTick(statistics_, FILTER_RANGE_CHECK);

    std::string start_key = lkey.ToString();
    std::string end_key = hkey.ToString();
    std::reverse(start_key.begin(), start_key.end());
    std::reverse(end_key.begin(), end_key.end());

    int probe_cnt = 0;

    size_t range_dist_bits = FindRangeDistance(start_key, end_key);
    // round to multiple bits_per_level
    if (range_dist_bits%bits_per_level_ != 0)
      range_dist_bits += (bits_per_level_ - range_dist_bits%bits_per_level_);
    if (range_dist_bits > range_dist_bits_thres_) {
      //printf("Prefix not long enough, range_dist_bits %d\n", range_dist_bits);
      RecordTick(statistics_, FILTER_RANGE_PREFIX_SHORT);
      return true;
    } else {
      std::string prefix = start_key;
      uint32_t *suffix = (uint32_t *)&prefix[0];
      *suffix = *suffix & (~0u << range_dist_bits);
      return RangeCheck(*(uint32_t *)start_key.data(), *(uint32_t *)end_key.data(), prefix, range_dist_bits-1, &probe_cnt);
    }
  }


 private:

 // Check range (lkey -> hkey) exist in filter (At most 1byte suffix)
  // prefix - prefix key
  // l - suffix index (multiple of bits_per_level_)
  bool RangeCheck(uint32_t lkey, uint32_t hkey, std::string prefix, int l, int *probe_cnt) {
    uint32_t *suffix = (uint32_t *)&prefix[0];
    if (*suffix > hkey || (*suffix+(1<<(l+1))-1) < lkey) {
      // prefix not in range
      return false;
    }
    if (*suffix >= lkey && (*suffix+(1<<(l+1))-1) <= hkey) {
      // prefix in range
      return Doubt(prefix, l, probe_cnt);
    }
    uint32_t orig_suffix = *suffix;
    for (int i = 0; i < (1<<bits_per_level_); i++) {
      *suffix = orig_suffix | (i<<(l-bits_per_level_+1));
      if (RangeCheck(lkey, hkey, prefix, l-bits_per_level_, probe_cnt)) return true;
    }
    return false;
  }

  // Check prefix subtree exist in filter
  // prefix - prefix key
  // l - suffix index (multiple of bits_per_level_)
  bool Doubt(std::string prefix, int l, int *probe_cnt) {
    if (l < -1) return true;

    if ((l+1)/bits_per_level_ < levels_) {
      RecordTick(statistics_, FILTER_RANGE_PROBES);
      (*probe_cnt)++;
      if (*probe_cnt > max_probes_) return true;
      // std::string check_key(prefix.rbegin(), prefix.rend());
      if (!KeyMayMatchLevel(prefix, (l+1)/bits_per_level_)) return false;
    }
    uint32_t *suffix = (uint32_t *)&prefix[0];
    uint32_t orig_suffix = *suffix;
    for (int i = 0; i < (1<<bits_per_level_); i++) {
      *suffix = orig_suffix | (i<<(l-bits_per_level_+1));
      if (Doubt(prefix, l-bits_per_level_, probe_cnt)) return true;
    }
    return false;
  }

 bool KeyMayMatchLevel(const Slice& key, int l)  {
    const size_t len = bf_[l].size();
    // printf("Check Bloom filter level %d (%d), key %llX \n",l, len, *(uint64_t *)key.data());
    if (len < 2) return false;

    const char* array = bf_[l].data();
    const size_t bits = (len - 1) * 8;

    // Use the encoded k so that we can read filters generated by
    // bloom filters created using different parameters.
    const size_t k = array[len - 1];
    if (k > 30) {
      // Reserved for potentially new encodings for short bloom filters.
      // Consider it a match.
      return true;
    }

    uint64_t h = BloomHash(key);
    const uint64_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
    for (size_t j = 0; j < k; j++) {
      const uint64_t bitpos = h % bits;
      if ((array[bitpos / 8] & (1 << (bitpos % 8))) == 0) {
        return false;
      }
      h += delta;
    }
    return true;
  }

  size_t FindRangeDistance (const Slice& lkey, const Slice& hkey) {
    assert(lkey.size() == hkey.size());
    size_t klen = lkey.size();
    size_t prefix_bits_len = 0;

    // uint64_t *l = (uint64_t*)(lkey.data());
    // uint64_t *h = (uint64_t*)(hkey.data());
    // for (int i = 0; i < klen/8; i++) {
    //   int64_t xor_64 = l[i] ^ h[i];
    //   if (xor_64 == 0) prefix_bits_len += 64;
    //   else {
    //     int leading_zeros = __builtin_clz(xor_64);
    //     return klen*8 - (prefix_bits_len+leading_zeros);
    //   }
    // }
    uint8_t *ll = (uint8_t*)(lkey.data());
    uint8_t *hh = (uint8_t*)(hkey.data());
    for (int i = 0; i < klen; i++) {
      uint8_t xor_8 = ll[klen-1-i] ^ hh[klen-1-i];
      if (xor_8 == 0) prefix_bits_len += 8;
      else {
        int leading_zeros = __builtin_clz(xor_8) - 24;
        return klen*8 - (prefix_bits_len+leading_zeros);
      }
    }
  }

  size_t num_keys_;
  size_t max_probes_;
  size_t range_dist_bits_thres_;
  size_t bits_per_level_;
  size_t levels_; // less than 8 due to memory budget
  size_t total_bits_per_key_;
  double *bits_per_key_;
  size_t *k_;
  size_t *bits_;
  std::string *bf_;
  Statistics *statistics_;
};


class RBloomFilter : public RangeFilter {
 public:

  RBloomFilter(int bits_per_key, int max_probes_bits, int num_keys, Statistics *stats) 
  : num_keys_(num_keys), max_probes_(1<<max_probes_bits - 1), bits_per_key_(bits_per_key), 
  statistics_(stats) {
    
    int filter_bytes = 0;
    // We intentionally round down to reduce probing cost a little bit
    k_ = static_cast<size_t>(bits_per_key_ * 0.69);  // 0.69 =~ ln(2)
    if (k_ < 1) k_ = 1;
    if (k_ > 30) k_ = 30;
    bits_ = static_cast<size_t>(num_keys * bits_per_key_);
    if (bits_ < 64) bits_ = 64;
    bits_ = (bits_ + 7) / 8 * 8;

    // filter data
    bf_.resize(bits_/8, 0);
    bf_.push_back(static_cast<char>(k_));
    filter_bytes += (bits_/8+1);
    
    printf("RBloomFilter #keys: %d, size: %.3f MB\n", num_keys, (double)filter_bytes/1024/1024);
  }

  ~RBloomFilter() {}

  virtual std::string GenFilterName() {
    return "/tmp/RBloomFilter_"+std::to_string(num_keys_)+"_"+std::to_string(bits_per_key_);
  }

  void LoadFilter(std::string filename) {
    std::ifstream filterFile;
    filterFile.open (filename.c_str(), std::ios::in | std::ios::binary);
    filterFile.read ((char *)bf_.data(), bf_.size());
    filterFile.close();
  }

  void SaveFilter(std::string filename) {
    std::ofstream filterFile;
    filterFile.open (filename.c_str(), std::ios::out | std::ios::binary);
    filterFile << bf_;
    filterFile.close();
  }
  
  void InsertItem(const Slice& key)  {
    std::string le_key = key.ToString();
    std::reverse(le_key.begin(), le_key.end());

    size_t klen = le_key.size();
    char* array = &(bf_[0]);
    // Use double-hashing to generate a sequence of hash values.
    // See analysis in [Kirsch,Mitzenmacher 2006].
    uint64_t h = BloomHash(le_key);
    const uint64_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
    for (size_t j = 0; j < k_; j++) {
      const uint64_t bitpos = h % bits_;
      array[bitpos / 8] |= (1 << (bitpos % 8));
      h += delta;
    }
  }

  bool KeyMayMatch(const Slice& key) {
    RecordTick(statistics_, FILTER_POINT_CHECK);
    std::string le_key = key.ToString();
    std::reverse(le_key.begin(), le_key.end());
    
    const size_t len = bf_.size();
    if (len < 2) return false;

    const char* array = bf_.data();
    const size_t bits = (len - 1) * 8;

    // Use the encoded k so that we can read filters generated by
    // bloom filters created using different parameters.
    const size_t k = array[len - 1];
    if (k > 30) {
      // Reserved for potentially new encodings for short bloom filters.
      // Consider it a match.
      return true;
    }

    uint64_t h = BloomHash(le_key);
    const uint64_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
    for (size_t j = 0; j < k; j++) {
      const uint64_t bitpos = h % bits;
      if ((array[bitpos / 8] & (1 << (bitpos % 8))) == 0) return false;
      h += delta;
    }
    return true;
  }


  bool RangeMayMatch(const Slice& lkey, const Slice& hkey) {
    RecordTick(statistics_, FILTER_RANGE_CHECK);

    std::string curr_key = lkey.ToString();
    std::string end_key = hkey.ToString();
    std::reverse(curr_key.begin(), curr_key.end());
    std::reverse(end_key.begin(), end_key.end());

    uint32_t *suffix = (uint32_t *)&curr_key[0];
    uint32_t end_suffix = *(uint32_t *)&end_key[0];
    for (int i = 0; i < max_probes_&&(*suffix)<=end_suffix; i++) {
      RecordTick(statistics_, FILTER_RANGE_PROBES);
      // std::string check_key (curr_key.rbegin(), curr_key.rend());
      if (KeyMayMatchReverse(curr_key)) return true;
      (*suffix)++;
    }
    if (*suffix > end_suffix) return false;
    else return true;
  }


 private:

 bool KeyMayMatchReverse(const Slice& key) {
    RecordTick(statistics_, FILTER_POINT_CHECK);
    const size_t len = bf_.size();
    if (len < 2) return false;

    const char* array = bf_.data();
    const size_t bits = (len - 1) * 8;

    // Use the encoded k so that we can read filters generated by
    // bloom filters created using different parameters.
    const size_t k = array[len - 1];
    if (k > 30) {
      // Reserved for potentially new encodings for short bloom filters.
      // Consider it a match.
      return true;
    }

    uint64_t h = BloomHash(key);
    const uint64_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
    for (size_t j = 0; j < k; j++) {
      const uint64_t bitpos = h % bits;
      if ((array[bitpos / 8] & (1 << (bitpos % 8))) == 0) return false;
      h += delta;
    }
    return true;
  }

  size_t num_keys_;
  size_t max_probes_;
  double bits_per_key_;
  size_t k_;
  size_t bits_;
  std::string bf_;
  Statistics *statistics_;
};


const BloomFilter* NewBloomFilter(int bits_per_key) {
  return new BloomFilter(bits_per_key);
}

const CounterBloomFilter* NewCounterBloomFilter(int cells_per_key, int width) {
  return new CounterBloomFilter(cells_per_key, width);
}

RangeFilter* NewHiBloomFilter(int bits_per_key, int bits_per_level, int levels, int exam_suffix_bits, int num_keys, int max_probes, Statistics *stats) {
  return new HiBloomFilter(bits_per_key, bits_per_level, levels, exam_suffix_bits, num_keys, max_probes, stats);
}

RangeFilter* NewRBloomFilter(int bits_per_key, int max_probes_bits, int num_keys, Statistics *stats) {
  return new RBloomFilter(bits_per_key, max_probes_bits, num_keys, stats);
}

}  // namespace kvrangedb
