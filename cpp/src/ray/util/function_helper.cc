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

#include "function_helper.h"
#include <dlfcn.h>
#include <stdio.h>
#include <string.h>
#include <memory>
#include "address_helper.h"
#include "ray/core.h"

namespace ray {
namespace api {

uintptr_t base_addr = 0;

static const uintptr_t BaseAddressForHandle(void *handle) {
  /// TODO(Guyang Song): Implement a cross-platform function.
  return (uintptr_t)((NULL == handle) ? NULL : (void *)*(size_t const *)(handle));
}

uintptr_t FunctionHelper::LoadLibrary(std::string lib_name) {
  /// Generate base address from library.
  RAY_LOG(INFO) << "Start load library " << lib_name;
  void *handle = dlopen(lib_name.c_str(), RTLD_LAZY);
  uintptr_t base_addr = BaseAddressForHandle(handle);
  RAY_CHECK(base_addr > 0);
  RAY_LOG(INFO) << "Loaded library " << lib_name << " to base address " << base_addr;
  loaded_library_.emplace(lib_name, base_addr);
  return base_addr;
}

uintptr_t FunctionHelper::GetBaseAddress(std::string lib_name) {
  auto got = loaded_library_.find(lib_name);
  if (got == loaded_library_.end()) {
    return LoadLibrary(lib_name);
  }
  return got->second;
}

}  // namespace api
}  // namespace ray