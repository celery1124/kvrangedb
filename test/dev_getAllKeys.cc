
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
  std::string dev_name = "/dev/nvme1n1";

  auto wcts = std::chrono::system_clock::now();
  kvssd::KVSSD *kvd_ = new kvssd::KVSSD(dev_name.c_str());
  std::vector<std::string> keys;
  kvd_->kv_scan_keys(keys);

  std::chrono::duration<double> wctduration = (std::chrono::system_clock::now() - wcts);
  printf("Total keys in device(%s) : %d\n", dev_name.c_str(), keys.size());
  std::cout << "Finished in " << wctduration.count() << " seconds [Wall Clock]" << std::endl;
  return 0; 
}
