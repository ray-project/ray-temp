// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstddef>
#include <cstdint>

#include "ray/object_manager/plasma/compat.h"
#include "ray/util/macros.h"

namespace plasma {

class ClientMmapTableEntry {
 public:
  ClientMmapTableEntry(MEMFD_TYPE fd, int64_t map_size);

  ~ClientMmapTableEntry();

  uint8_t *pointer() { return pointer_; }

  MEMFD_TYPE fd() { return fd_; }

 private:
  /// The associated file descriptor on the client.
  MEMFD_TYPE fd_;
  /// The result of mmap for this file descriptor.
  uint8_t *pointer_;
  /// The length of the memory-mapped file.
  size_t length_;

  RAY_DISALLOW_COPY_AND_ASSIGN(ClientMmapTableEntry);
};

}  // namespace plasma
