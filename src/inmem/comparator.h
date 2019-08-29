/******* kvrangedb *******/
/* comparator.h
* 08/21/2019
* by Mian Qin
*/

#ifndef _inmem_comparator_h_
#define _inmem_comparator_h_

#include "kvrangedb/slice.h"

namespace inmem {

class Slice;

// A Comparator object provides a total order across slices that are
// used as keys in an sstable or a database.  A Comparator implementation
// must be thread-safe since kvrangedb may invoke its methods concurrently
// from multiple threads.
class Comparator {
 public:
  virtual ~Comparator() {};

  // Three-way comparison.  Returns value:
  //   < 0 iff "a" < "b",
  //   == 0 iff "a" == "b",
  //   > 0 iff "a" > "b"
  virtual int Compare(const Slice& a, const Slice& b) const = 0;

};

}  // namespace inmem


#endif