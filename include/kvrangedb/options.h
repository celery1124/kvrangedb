/******* kvrangedb *******/
/* options.h
* 07/23/2019
* by Mian Qin
*/

#ifndef _options_h_
#define _options_h_


#include <stddef.h>
#include <stdlib.h>
#include "kvrangedb/comparator.h"

namespace kvrangedb {

class Comparator;
class Slice;

enum IndexType {
  LSM,
  BTREE,
  BASE
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
  IndexType indexType;
  
  // Whether enable value prefetch for iterators
  // Default: false
  bool prefetchEnabled;

  // Prefetch buffer size
  // Default: 16
  int prefetchDepth;

  // Whether enable range filter for LSM index
  // Default: false
  bool rangefilterEnabled;

  Options() : comparator(BytewiseComparator()),
              indexType(LSM),
              prefetchEnabled(false),
              prefetchDepth(16),
              rangefilterEnabled(false) {
    // Load from environment variable
    char *env_p;
    if(env_p = std::getenv("INDEX_TYPE")) {
      if (strcmp(env_p, "LSM") == 0)
        indexType = LSM;
      else if (strcmp(env_p, "BTREE") == 0)
        indexType = BTREE;
      else if (strcmp(env_p, "BASE") == 0)
        indexType = BASE;
      else
        indexType = LSM;
    }

    if(env_p = std::getenv("PREFETCH_ENA")) {
      if (strcmp(env_p, "TRUE") == 0)
        prefetchEnabled = true;
      else
        prefetchEnabled = false;
    }

    if(env_p = std::getenv("PREFETCH_DEPTH")) {
      prefetchDepth = atoi(env_p);
    }

    if(env_p = std::getenv("RANGE_FILTER_ENA")) {
      if (strcmp(env_p, "TRUE") == 0)
        rangefilterEnabled = true;
      else
        rangefilterEnabled = false;
    }
      
  };
};


// Options that control read operations
struct ReadOptions {
  const Slice* upper_key;

  ReadOptions()
      : upper_key(NULL) {
  }
};

// Options that control write operations
struct WriteOptions {
  // If true, the write will be flushed from the operating system
  // buffer cache (by calling WritableFile::Sync()) before the write
  // is considered complete.  If this flag is true, writes will be
  // slower.
  //
  // If this flag is false, and the machine crashes, some recent
  // writes may be lost.  Note that if it is just the process that
  // crashes (i.e., the machine does not reboot), no writes will be
  // lost even if sync==false.
  //
  // In other words, a DB write with sync==false has similar
  // crash semantics as the "write()" system call.  A DB write
  // with sync==true has similar crash semantics to a "write()"
  // system call followed by "fsync()".
  //
  // Default: false
  bool sync;

  WriteOptions()
      : sync(false) {
  }
};

}  // namespace kvrangedb

#endif
