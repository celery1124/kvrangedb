// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "kvssd/kvssd.h"

using namespace rocksdb;

std::string kDBPath = "/tmp/rocksdb_kvssd_example";

int main() {
  kvssd::KVSSD *kvd = new kvssd::KVSSD("/dev/nvme2n1");

  DB* db;
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.paranoid_checks = false;
  options.max_open_files = 1000;
  options.env = rocksdb::NewKVEnvOpt(rocksdb::Env::Default(), kvd);
  //options.env = rocksdb::NewMemEnv(rocksdb::Env::Default());
  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  // Put key-value
  WriteOptions wopt;
  wopt.disableWAL = true;
  s = db->Put(wopt, "key1", "value");
  assert(s.ok());
  std::string value;
  // get value
  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.ok());
  assert(value == "value");


  delete db;

  return 0;
}
