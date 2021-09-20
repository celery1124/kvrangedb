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
#include <thread>

namespace kvrangedb {

enum Tickers : uint32_t {
  // request
  REQ_PUT = 0,
  REQ_GET,
  REQ_DEL,
  REQ_SEEK,
  REQ_NEXT,
  // Pack
  PACK_CNT,
  PACK_RECORD_CNT,
  PACK_BYTES,
  // I/O
  IO_PUT,
  IO_GET,
  IO_GET_NONE,
  IO_DEL,
  IO_APPEND,
  IO_PUT_BYTES,
  IO_GET_BYTES,
  IO_APP_BYTES,
  DEV_UTIL,
  // Write buffer
  WBUFFER_HIT,
  WBUFFER_MISS,
  WBUFFER_FILL,
  WBUFFER_ERASE,
  // Cache
  CACHE_HIT,
  CACHE_MISS,
  CACHE_FILL,
  CACHE_ERASE,
  // Filter
  FILTER_RANGE_CHECK,
  FILTER_POINT_CHECK,
  FILTER_POINT_POSITIVE,
  FILTER_POINT_NEGATIVE,
  FILTER_RANGE_PREFIX_SHORT,
  FILTER_RANGE_PROBES,
  FILTER_RANGE_POSITIVE,
  FILTER_RANGE_NEGATIVE,
  TICKER_ENUM_MAX
};

const std::vector<std::pair<Tickers, std::string>> TickersNameMap = {
    {REQ_PUT, "req.put"},
    {REQ_GET, "req.get"},
    {REQ_DEL, "req.delete"},
    {REQ_SEEK, "req.seek"},
    {REQ_NEXT, "req.next"},
    {PACK_CNT, "pack.cnt"},
    {PACK_RECORD_CNT, "pack.record.cnt"},
    {PACK_BYTES, "pack.bytes"},
    {IO_PUT, "io.put"},
    {IO_GET, "io.get"},
    {IO_GET_NONE, "io.get.non-exist"},
    {IO_DEL, "io.delete"},
    {IO_APPEND, "io.append"},
    {IO_PUT_BYTES, "io.put.bytes"},
    {IO_GET_BYTES, "io.get.bytes"},
    {IO_APP_BYTES, "io.append.bytes"},
    {DEV_UTIL, "dev.util"},
    {WBUFFER_HIT, "write.buffer.hit"},
    {WBUFFER_MISS, "write.buffer.miss"},
    {WBUFFER_FILL, "write.buffer.fill"},
    {WBUFFER_ERASE, "write.buffer.erase"},
    {CACHE_HIT, "cache.hit"},
    {CACHE_MISS, "cache.miss"},
    {CACHE_FILL, "cache.fill"},
    {CACHE_ERASE, "cache.erase"},
    {FILTER_RANGE_CHECK, "filter.range.check"},
    {FILTER_POINT_CHECK, "filter.point.check"},
    {FILTER_POINT_POSITIVE, "filter.point.positive"},
    {FILTER_POINT_NEGATIVE, "filter.point.negative"},
    {FILTER_RANGE_PREFIX_SHORT, "filter.range.prefix.short"},
    {FILTER_RANGE_PROBES, "filter.range.probes"},
    {FILTER_RANGE_POSITIVE, "filter.range.positive"},
    {FILTER_RANGE_NEGATIVE, "filter.range.negative"},
};

class Statistics {
public:
  std::atomic_ullong tickers_[TICKER_ENUM_MAX] = {{0}};
  std::thread *report_;
  pthread_t  report_tt_;
  int interval_;
public:
  Statistics() : interval_(-1) {}
  ~Statistics() {
    if (interval_ > 0) {
      pthread_cancel(report_tt_);
      reportStats();
      delete report_;
    }
    else 
      reportStats();
  }
  void setStatsDump(int interval) {
    interval_ = interval;
    if (interval > 0) {
      report_ = new std::thread(&Statistics::ReportStats, this, interval);
      report_tt_ = report_->native_handle();
      report_->detach();
    }
  }

  void recordTick(uint32_t tickType, uint64_t count = 1) {
    if (tickType < TICKER_ENUM_MAX)
      tickers_[tickType].fetch_add(count, std::memory_order_relaxed);
  }
  
  uint64_t getTickCount(uint32_t tickType) {
    return tickers_[tickType].load(std::memory_order_relaxed);
  }

  void Reset() {
    for (uint32_t tick; tick < TICKER_ENUM_MAX; tick++)
      tickers_[tick].store(0, std::memory_order_seq_cst);
  }

  void reportStats() {
    time_t timer;
    char buffer[26];
    struct tm* tm_info;

    timer = time(NULL);
    tm_info = localtime(&timer);
    strftime(buffer, 26, "%Y-%m-%d %H:%M:%S", tm_info);

    fprintf(stderr, "[%s] ", buffer);
    for(uint32_t i = 0; i < TickersNameMap.size(); i++) {
      fprintf(stderr, "\t%s: %lu", TickersNameMap[i].second.c_str(), getTickCount(i));
      if ((i+1)%6 == 0) fprintf(stderr, "\n\t\t");
    }
    fprintf(stderr, "\n");
  }
 
  void ReportStats(int interval) {
    while (true) {
      std::this_thread::sleep_for (std::chrono::seconds(interval));
      reportStats();
    }
  }

};

inline void RecordTick(Statistics* statistics, uint32_t ticker_type,
                       uint64_t count = 1) {
  if (statistics) {
    statistics->recordTick(ticker_type, count);
  }
}

inline uint64_t GetTickerCount(Statistics* statistics, uint32_t ticker_type) {
  if (statistics) {
    return statistics->getTickCount(ticker_type);
  }
  return 0;
}

} // end of namespace
