#include <getopt.h>
#include <stdio.h>

#include <chrono>
#include <thread>
#include <iostream>

#include "ray/object_manager/plasma/store.h"
// TODO(pcm): Convert getopt and sscanf in the store to use more idiomatic C++
// and get rid of the next three lines:
#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

int main(int argc, char *argv[]) {
  std::string socket_name;
  // Directory where plasma memory mapped files are stored.
  std::string plasma_directory;
  std::string external_store_endpoint;
  bool hugepages_enabled = false;
  bool keep_idle = false;
  int64_t system_memory = -1;
  int c;
  while ((c = getopt(argc, argv, "s:m:d:e:h:z")) != -1) {
    switch (c) {
      case 'd':
        plasma_directory = std::string(optarg);
        break;
      case 'e':
        external_store_endpoint = std::string(optarg);
        break;
      case 'h':
        hugepages_enabled = true;
        break;
      case 's':
        socket_name = std::string(optarg);
        break;
      case 'm': {
        char extra;
        int scanned = sscanf(optarg, "%" SCNd64 "%c", &system_memory, &extra);
        ARROW_CHECK(scanned == 1);
        break;
      }
      case 'z': {
        keep_idle = true;
        break;
      }
      default:
        exit(-1);
    }
  }

  if (!keep_idle) {
    plasma::PlasmaStoreRunner runner(socket_name, system_memory, hugepages_enabled,
                                     plasma_directory, external_store_endpoint);
    runner.Start();
  } else {
    printf("The Plasma Store is started with the '-z' flag, and it will run idle as a placeholder.");
    while (true) {
        std::this_thread::sleep_for(std::chrono::hours(1000));
    }
  }

  return 0;
}
