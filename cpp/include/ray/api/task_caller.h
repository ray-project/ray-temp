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

#include "ray/core.h"

namespace ray {
namespace api {

template <typename ReturnType>
class TaskCaller {
 public:
  TaskCaller();

  TaskCaller(RayRuntime *runtime, RemoteFunctionPtrHolder ptr,
             std::vector<std::unique_ptr<::ray::TaskArg>> &&args);

  ObjectRef<ReturnType> Remote();

 private:
  RayRuntime *runtime_;
  RemoteFunctionPtrHolder ptr_;
  std::vector<std::unique_ptr<::ray::TaskArg>> args_;
};

// ---------- implementation ----------

template <typename ReturnType>
TaskCaller<ReturnType>::TaskCaller() {}

template <typename ReturnType>
TaskCaller<ReturnType>::TaskCaller(RayRuntime *runtime, RemoteFunctionPtrHolder ptr,
                                   std::vector<std::unique_ptr<::ray::TaskArg>> &&args)
    : runtime_(runtime), ptr_(ptr), args_(std::move(args)) {}

template <typename ReturnType>
ObjectRef<ReturnType> TaskCaller<ReturnType>::Remote() {
  auto returned_object_id = runtime_->Call(ptr_, args_);
  return ObjectRef<ReturnType>(returned_object_id);
}
}  // namespace api
}  // namespace ray
