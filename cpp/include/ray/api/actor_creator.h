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

template <typename ActorType>
class ActorCreator {
 public:
  ActorCreator();

  ActorCreator(RayRuntime *runtime, RemoteFunctionPtrHolder ptr,
               std::vector<std::unique_ptr<::ray::TaskArg>> &&args);

  ActorHandle<ActorType> Remote();

 private:
  RayRuntime *runtime_;
  RemoteFunctionPtrHolder ptr_;
  std::vector<std::unique_ptr<::ray::TaskArg>> args_;
};

// ---------- implementation ----------

template <typename ActorType>
ActorCreator<ActorType>::ActorCreator() {}

template <typename ActorType>
ActorCreator<ActorType>::ActorCreator(RayRuntime *runtime, RemoteFunctionPtrHolder ptr,
                                      std::vector<std::unique_ptr<::ray::TaskArg>> &&args)
    : runtime_(runtime), ptr_(ptr), args_(std::move(args)) {}

template <typename ActorType>
ActorHandle<ActorType> ActorCreator<ActorType>::Remote() {
  auto returned_actor_id = runtime_->CreateActor(ptr_, args_);
  return ActorHandle<ActorType>(returned_actor_id);
}
}  // namespace api
}  // namespace ray
