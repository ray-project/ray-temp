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

#include <ray/api/wait_result.h>

#include <cstdint>
#include <memory>
#include <msgpack.hpp>
#include <typeinfo>
#include <vector>

#include "ray/core.h"

namespace ray {
namespace api {

struct MemberFunctionPtrHolder {
  uintptr_t value[2];
};

struct RemoteFunctionPtrHolder {
  /// The remote function pointer
  uintptr_t function_pointer;
  /// The executable function pointer
  uintptr_t exec_function_pointer;
};

class RayRuntime {
 public:
  virtual ObjectID Put(std::shared_ptr<msgpack::sbuffer> data) = 0;
  virtual std::shared_ptr<msgpack::sbuffer> Get(const ObjectID &id) = 0;

  virtual std::vector<std::shared_ptr<msgpack::sbuffer>> Get(
      const std::vector<ObjectID> &ids) = 0;

  virtual WaitResult Wait(const std::vector<ObjectID> &ids, int num_objects,
                          int timeout_ms) = 0;

  virtual ObjectID Call(const RemoteFunctionPtrHolder &fptr,
                        std::vector<std::unique_ptr<::ray::TaskArg>> &args) = 0;
  virtual ActorID CreateActor(const RemoteFunctionPtrHolder &fptr,
                              std::vector<std::unique_ptr<::ray::TaskArg>> &args) = 0;
  virtual ObjectID CallActor(const RemoteFunctionPtrHolder &fptr, const ActorID &actor,
                             std::vector<std::unique_ptr<::ray::TaskArg>> &args) = 0;
};

}  // namespace api
}  // namespace ray