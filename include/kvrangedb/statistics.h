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
  // I/O
  TICKER_ENUM_MAX
};

const std::vector<std::pair<Tickers, std::string>> TickersNameMap = {
    {REQ_PUT, "req.put"},
    {REQ_GET, "req.get"},
    {REQ_DEL, "req.delete"},
};

class Statistics {
public:
  std::atomic_uint_fast64_t tickers_[TICKER_ENUM_MAX] = {{0}};
  std::thread *report_;
public:
  Statistics(int interval) {
    report_ = new std::thread(&Statistics::ReportStats, this, interval);
  }
  ~Statistics() {delete report_;}

  void recordTick(uint32_t tickType, uint64_t count = 1) {
    if (tickType < TICKER_ENUM_MAX)
      tickers_[tickType].fetch_add(count);
  }
  
  uint64_t getTickCount(uint32_t tickType) {
    return tickers_[tickType].load();
  }

  void reportStats() {
    time_t timer;
    char buffer[26];
    struct tm* tm_info;

    timer = time(NULL);
    tm_info = localtime(&timer);
    strftime(buffer, 26, "%Y-%m-%d %H:%M:%S", tm_info);

    printf("[%s] ", buffer);
    for(int i = 0; i < TickersNameMap.size(); i++) {
      printf("\t%s, %lu", TickersNameMap[i].second.c_str(), getTickCount(i));
    }
    printf("\n");
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

inline void GetTickerCount(Statistics* statistics, uint32_t ticker_type) {
  if (statistics) {
    statistics->getTickCount(ticker_type);
  }
}

} // end of namespace
