
/* db_test.cc
* 07/29/2019
* by Mian Qin
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <new>
#include <assert.h>
#include <unistd.h>
#include <thread>
#include <map>
#include <ctime>
#include <chrono>
#include <iostream>
#include <mutex>
#include <vector>
#include <string>

#include "kvssd/kvssd.h"

int main () {
  kvssd::KVSSD *kvd_ = new kvssd::KVSSD("/dev/nvme1n1");
  std::vector<std::string> keys;
  kvd_->kv_scan_keys(keys);

  printf("Total keys in device(%s) : %d\n", keys.size());
  return 0; 
}
