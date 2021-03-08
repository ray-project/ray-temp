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
#include <dlfcn.h>
#include <stdint.h>

namespace ray {
namespace api {

/// A base address which is used to calculate function offset
extern uintptr_t dynamic_library_base_addr;

/// Get the base address of libary which the function address belongs to.
uintptr_t GetBaseAddressOfLibraryFromAddr(void *addr);
}  // namespace api
}  // namespace ray