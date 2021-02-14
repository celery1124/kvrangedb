/******* kvrangedb *******/
/* options.h
* 07/23/2019
* by Mian Qin
*/

#ifndef _options_h_
#define _options_h_


#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <memory>
#include <string>

#include "kvrangedb/comparator.h"
#include "kvrangedb/statistics.h"

namespace kvrangedb {

class Comparator;
class Slice;

enum IndexType {
  LSM,
  LSMOPT,
  ROCKS,
  BTREE,
  BASE,
	INMEM
};

enum RangeFilterType {
  NoFilter,
  HiBloom,
  RBloom
};

// Options to control the behavior of a database (passed to DB::Open)
struct Options {
  // -------------------
  // Parameters that affect behavior

  // Comparator used to define the order of keys in the table.
  // Default: a comparator that uses lexicographic byte-wise ordering
  //
  // REQUIRES: The client must ensure that the comparator supplied
  // here has the same name and orders keys *exactly* the same as the
  // comparator provided to previous open calls on the same DB.
  const Comparator* comparator;

  // Ordered key index type
  // Default: LSM structured ordered key index

  // LSM -> LSM Tress structure index
  // BTREE -> B Tree like external structure using K-V interface
  // BASE -> Retrieve all keys from device (random order), then sort
	// INMEM -> Keep sorted keys in-memory, assume apps keep order
  IndexType indexType;

  // Whether clean LSM, B-Tree meta KV
  // LSM -> "/CURRENT"
  bool cleanIndex;

  // Number of index partitions, MAX 8 (currently only support LSM)
  // Default: 1 (no partition)
  int indexNum;

  // Index cache size in MB (currently only support LSM)
  // Default: 128MB
  int indexCacheSize;
  
  // Whether enable value prefetch for iterators
  // Default: false
  bool prefetchEnabled;

  // Prefetch buffer size
  // Default: 128
  int prefetchDepth;

  // Whether enable range filter for LSM index
  // Default: false
  bool rangefilterEnabled;

  // Helper record hint
  int helperHint;
  int helperTrainingThres;

  // Pack size for physical KV
  // Default: 4096
  int packSize;

  // Value size threshold for packing
  // Default: 4096
  int packThres;

  // Max number of KVs for packing
  // Default: 8
  int maxPackNum;

  // Timeout for packing thread dequeue in us
  // Default: 5000
  int packDequeueTimeout;

  // Depth of the packed KV queue
  // Default: 1024
  int packQueueDepth;

  // Number of threads to write packed KV
  // Default: 8
  int packThreadsNum;

  // Manual compaction
  // Default: false
  bool manualCompaction;

  // Background compaction
  // Default: false
  bool bgCompaction;

  // Background compaction interval (sec)
  // Default: 10
  bool bgCompactionInterval;

  // Background compaction scan length
  // Default: 100000
  bool bgCompactionScanLength;

  // Hot key training count
  // Default: 1000000
  int hotKeyTrainingNum;

  // Bits per key for bloom filter
  // Default: 8
  int filterBitsPerKey;

  // In-memory data cache size
  // Default: 16 (MB)
  int dataCacheSize;

  // Range filter type
  // Default: HiBloom
  RangeFilterType rfType;

  // Range filter number of keys
  // Default: 
  int rfNumKeys;

  // Range filter bits per key
  // Default: 16
  int rfBitsPerKey;

  // Range filter exam suffix bits
  // Default: 16
  int rfExamBits;

  // Range filter levels (HiBloom)
  // Default: 1
  int rfLevels;

  // Range filter bits per level (HiBloom)
  // Default: 1
  int rfBitsPerLevel;

  // Statistic (create to record count)
  // Default: NULL
  std::shared_ptr<Statistics> statistics;

  // Statistic dump interval in seconds
  // Default: -1 (no dump)
  int stats_dump_interval;

  Options() : comparator(BytewiseComparator()),
              indexType(LSM),
              cleanIndex(false),
              indexNum(1),
              indexCacheSize(128),
              prefetchEnabled(false),
              prefetchDepth(128),
              rangefilterEnabled(false),
              helperHint(0),
              helperTrainingThres(10),
              packSize(4096),
              packThres(4096),
              maxPackNum(8),
              packDequeueTimeout(5000),
              packQueueDepth(1024),
              packThreadsNum(8),
              manualCompaction(false),
              bgCompaction(false),
              bgCompactionInterval(10),
              bgCompactionScanLength(100000),
              hotKeyTrainingNum(1000000),
              filterBitsPerKey(8),
              dataCacheSize(16),
              rfType(NoFilter),
              rfNumKeys(1000000000),
              rfBitsPerKey(16),
              rfExamBits(16),
              rfLevels(1),
              rfBitsPerLevel(1),
              statistics(nullptr),
              stats_dump_interval(-1) {
    // Load from environment variable
    char *env_p;
    if(env_p = std::getenv("INDEX_TYPE")) {
      if (strcmp(env_p, "LSM") == 0)
        indexType = LSM;
      else if (strcmp(env_p, "LSMOPT") == 0)
        indexType = LSMOPT;
      else if (strcmp(env_p, "ROCKS") == 0)
        indexType = ROCKS;
      else if (strcmp(env_p, "BTREE") == 0)
        indexType = BTREE;
      else if (strcmp(env_p, "BASE") == 0)
        indexType = BASE;
			else if (strcmp(env_p, "INMEM") == 0)
				indexType = INMEM;
      else
        indexType = ROCKS; // default
    }

    if(env_p = std::getenv("INDEX_NUM")) {
      indexNum = atoi(env_p);
    }

    if(env_p = std::getenv("INDEX_CACHE_SIZE")) {
      indexCacheSize = atoi(env_p);
    }

    if(env_p = std::getenv("PREFETCH_ENA")) {
      if (strcmp(env_p, "TRUE") == 0 || strcmp(env_p, "true") == 0)
        prefetchEnabled = true;
      else
        prefetchEnabled = false;
    }

    if(env_p = std::getenv("PREFETCH_DEPTH")) {
      prefetchDepth = atoi(env_p);
    }

    if(env_p = std::getenv("RANGE_FILTER_ENA")) {
      if (strcmp(env_p, "TRUE") == 0 || strcmp(env_p, "true") == 0)
        rangefilterEnabled = true;
      else
        rangefilterEnabled = false;
    }
    
    if (env_p = std::getenv("HELPER_HINT")) {
      if (strcmp(env_p, "training") == 0 || strcmp(env_p, "train") == 0)
        helperHint = 1;
      else if (strcmp(env_p, "infer") == 0 || strcmp(env_p, "INFER") == 0)
        helperHint = 2;
      else 
        helperHint = 0;
    }

    if (env_p = std::getenv("HELPER_TRAINING_THRES")) {
      helperTrainingThres = atoi(env_p);
    }
    if (env_p = std::getenv("PACK_DEQUEUE_TIMEOUT")) {
      packDequeueTimeout = atoi(env_p);
    }

    if(env_p = std::getenv("MANUAL_COMPACTION")) {
      if (strcmp(env_p, "TRUE") == 0 || strcmp(env_p, "true") == 0)
        manualCompaction = true;
      else
        manualCompaction = false;
    }
  };
  static std::shared_ptr<Statistics> CreateDBStatistics() {
    printf("KVRangeDB Statistics Created\n");
    return std::make_shared<Statistics>();
  } 
};

// Options that control read operations
struct ReadOptions {
  // Define the upper key (Non-Inclusive) for range query
  // Default: NULL
  Slice* upper_key;

  // Potential user hint for the length of a scan (how many next after seek?)
  // Default: 1 (adptively increase)
  int scan_length;

  // Potential user hint for the size of the value (packed or unpacked?)
  // 0 -> auto, 1 -> unpacked, 2 -> packed
  // Default: 0 (no hints)
  int hint_packed;

  // // Buffer size for base iterator in Bytes
  // // Default: 4MB
  // int base_iter_buffer_size;

  ReadOptions()
      : upper_key(NULL),
        scan_length(1),
        hint_packed(0) {
  }
};

// Options that control write operations
struct WriteOptions {
  // From LevelDB write options, currently we don't use this
  // Default: false
  bool sync;
  // Write Index in batch
  // Default: false
  bool batchIDXWrite;
  // Batch size for batch index write
  // Default: 8
  size_t batchIDXSize;

  WriteOptions()
      : sync(false),
        batchIDXWrite(false),
        batchIDXSize(8) {
  }
};

}  // namespace kvrangedb

#endif
