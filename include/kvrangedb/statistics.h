/******* kvrangedb *******/
/* statistics.h
* 07/23/2020
* by Mian Qin
*/


#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace kvrangedb {

enum Tickers : uint32_t {
  // request
  REQ_PUT = 0,
  REQ_GET,
  REQ_DEL,
  // I/O
  TICKER_ENUM_MAX
};

class Statistics {
public:
  std::atomic_uint_fast64_t tickers_[TICKER_ENUM_MAX] = {{0}};
public:
  Statistics() {}

  void recordTick(uint32_t tickType, uint64_t count = 1) {
    if (tickType < TICKER_ENUM_MAX)
      tickers_[tickType].fetch_add(count);
  }
  
  uint64_t getTickCount(uint32_t tickType) {
    return tickers_[tickType].load();
  }
};

inline void RecordTick(Statistics* statistics, uint32_t ticker_type,
                       uint64_t count = 1) {
  if (statistics) {
    statistics->recordTick(ticker_type, count);
  }
}

inline void GetTickerCount(Statistics* statistics, uint32_t ticker_type) {
  if (statistics) {
    statistics->getTickCount(ticker_type);
  }
}

} // end of namespace
