/******* kvrangedb *******/
/* options.h
* 07/23/2019
* by Mian Qin
*/

#ifndef _options_h_
#define _options_h_


#include <stddef.h>
#include "kvrangedb/comparator.h"

namespace kvrangedb {

class Comparator;
class Slice;

enum IndexType {
  LSM,
  BTREE
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
  IndexType indexType;
  Options() : indexType(LSM) {
    comparator = BytewiseComparator();
  };
};


// Options that control read operations
struct ReadOptions {
  const Slice* lower_key;
  const Slice* upper_key;

  ReadOptions()
      : lower_key(NULL),
        upper_key(NULL) {
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