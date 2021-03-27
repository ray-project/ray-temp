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

#include <ray/api/object_ref.h>
#include "ray/core.h"

namespace ray {
namespace api {

template <typename ReturnType>
class ActorTaskCaller {
 public:
  ActorTaskCaller();

  ActorTaskCaller(RayRuntime *runtime, ActorID id, RemoteFunctionPtrHolder ptr,
                  std::vector<std::unique_ptr<::ray::TaskArg>> &&args);

  ObjectRef<ReturnType> Remote();

 private:
  RayRuntime *runtime_;
  ActorID id_;
  RemoteFunctionPtrHolder ptr_;
  std::vector<std::unique_ptr<::ray::TaskArg>> args_;
};

// ---------- implementation ----------

template <typename ReturnType>
ActorTaskCaller<ReturnType>::ActorTaskCaller() {}

template <typename ReturnType>
ActorTaskCaller<ReturnType>::ActorTaskCaller(
    RayRuntime *runtime, ActorID id, RemoteFunctionPtrHolder ptr,
    std::vector<std::unique_ptr<::ray::TaskArg>> &&args)
    : runtime_(runtime), id_(id), ptr_(ptr), args_(std::move(args)) {}

template <typename ReturnType>
ObjectRef<ReturnType> ActorTaskCaller<ReturnType>::Remote() {
  auto returned_object_id = runtime_->CallActor(ptr_, id_, args_);
  return ObjectRef<ReturnType>(returned_object_id);
}
}  // namespace api
}  // namespace ray
