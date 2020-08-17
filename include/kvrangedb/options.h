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
  BASE,
	INMEM
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
  
  // Whether enable value prefetch for iterators
  // Default: false
  bool prefetchEnabled;

  // Prefetch buffer size
  // Default: 16
  int prefetchDepth;

  // Whether enable range filter for LSM index
  // Default: false
  bool rangefilterEnabled;

  // Helper record hint
  int helperHint;
  int helperTrainingThres;

  Options() : comparator(BytewiseComparator()),
              indexType(LSM),
              prefetchEnabled(false),
              prefetchDepth(16),
              rangefilterEnabled(false),
              helperHint(0),
              helperTrainingThres(10) {
    // Load from environment variable
    char *env_p;
    if(env_p = std::getenv("INDEX_TYPE")) {
      if (strcmp(env_p, "LSM") == 0)
        indexType = LSM;
      else if (strcmp(env_p, "BTREE") == 0)
        indexType = BTREE;
      else if (strcmp(env_p, "BASE") == 0)
        indexType = BASE;
			else if (strcmp(env_p, "INMEM") == 0)
				indexType = INMEM;
      else
        indexType = LSM;
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
  };
};


// Options that control read operations
struct ReadOptions {
  // Define the upper key (Non-Inclusive) for range query
  // Default: NULL
  const Slice* upper_key;

  ReadOptions()
      : upper_key(NULL) {
  }
};

// Options that control write operations
struct WriteOptions {
  // From LevelDB write options, currently we don't use this
  // Default: false
  bool sync;

  WriteOptions()
      : sync(false) {
  }
};

}  // namespace kvrangedb

#endif
