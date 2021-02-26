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

#include "native_ray_runtime.h"

#include <ray/api.h>

#include "../util/address_helper.h"
#include "./object/native_object_store.h"
#include "./object/object_store.h"
#include "./task/native_task_submitter.h"

namespace ray {
namespace api {

NativeRayRuntime::NativeRayRuntime(std::shared_ptr<RayConfig> config) {
  config_ = config;
  object_store_ = std::unique_ptr<ObjectStore>(new NativeObjectStore(*this));
  task_submitter_ = std::unique_ptr<TaskSubmitter>(new NativeTaskSubmitter());
  task_executor_ = std::unique_ptr<TaskExecutor>(new TaskExecutor(*this));
}

}  // namespace api
}  // namespace ray