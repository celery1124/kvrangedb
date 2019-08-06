/******* kvrangedb *******/
/* db_iter.h
* 08/06/2019
* by Mian Qin
*/

#ifndef _kvrangedb_db_iter_h_
#define _kvrangedb_db_iter_h_

#include "kvrangedb/iterator.h"
#include "db_impl.h"

namespace kvrangedb {

Iterator* NewDBIterator(DBImpl *db, const ReadOptions &options);
} // end namespace kvrangedb

#endif