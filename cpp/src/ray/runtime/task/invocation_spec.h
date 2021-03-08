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

#include <ray/api/ray_runtime.h>
#include <msgpack.hpp>

#include "ray/core.h"

namespace ray {
namespace api {

class InvocationSpec {
 public:
  TaskType task_type;
  TaskID task_id;
  std::string name;
  ActorID actor_id;
  int actor_counter;
  std::string lib_name;
  RemoteFunctionPtrHolder fptr;
  std::vector<std::unique_ptr<::ray::TaskArg>> args;
};
}  // namespace api
}  // namespace ray
