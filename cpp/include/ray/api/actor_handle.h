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

#include <ray/api/actor_task_caller.h>
#include <ray/api/arguments.h>
#include <ray/api/exec_funcs.h>
#include <ray/api/ray_runtime_holder.h>

#include "ray/core.h"

namespace ray {
namespace api {

template <typename T>
struct FilterArgType {
  using type = T;
};

template <typename T>
struct FilterArgType<ObjectRef<T>> {
  using type = T;
};

template <typename ActorType, typename ReturnType, typename... Args>
using ActorFunc = ReturnType (ActorType::*)(Args...);

/// A handle to an actor which can be used to invoke a remote actor method, with the
/// `Call` method.
/// \param ActorType The type of the concrete actor class.
/// Note, the `Call` method is defined in actor_call.generated.h.
template <typename ActorType>
class ActorHandle {
 public:
  ActorHandle();

  ActorHandle(const ActorID &id);

  /// Get a untyped ID of the actor
  const ActorID &ID() const;

  /// Include the `Call` methods for calling remote functions.

  template <typename ReturnType, typename... Args>
  ActorTaskCaller<ReturnType> Task(
      ActorFunc<ActorType, ReturnType, typename FilterArgType<Args>::type...> actor_func,
      Args... args);

  /// Make ActorHandle serializable
  MSGPACK_DEFINE(id_);

 private:
  ActorID id_;
};

// ---------- implementation ----------
template <typename ReturnType, typename ActorType, typename FuncType,
          typename ExecFuncType, typename... ArgTypes>
inline ActorTaskCaller<ReturnType> CallActorInternal(FuncType &actor_func,
                                                     ExecFuncType &exec_func,
                                                     ActorHandle<ActorType> &actor,
                                                     ArgTypes &... args) {
  std::vector<std::unique_ptr<::ray::TaskArg>> task_args;
  Arguments::WrapArgs(&task_args, args...);
  RemoteFunctionPtrHolder ptr;
  MemberFunctionPtrHolder holder = *(MemberFunctionPtrHolder *)(&actor_func);
  ptr.function_pointer = reinterpret_cast<uintptr_t>(holder.value[0]);
  ptr.exec_function_pointer = reinterpret_cast<uintptr_t>(exec_func);
  return ActorTaskCaller<ReturnType>(internal::RayRuntime().get(), actor.ID(), ptr,
                                     std::move(task_args));
}

template <typename ActorType>
ActorHandle<ActorType>::ActorHandle() {}

template <typename ActorType>
ActorHandle<ActorType>::ActorHandle(const ActorID &id) {
  id_ = id;
}

template <typename ActorType>
const ActorID &ActorHandle<ActorType>::ID() const {
  return id_;
}

template <typename ActorType>
template <typename ReturnType, typename... Args>
ActorTaskCaller<ReturnType> ActorHandle<ActorType>::Task(
    ActorFunc<ActorType, ReturnType, typename FilterArgType<Args>::type...> actor_func,
    Args... args) {
  return CallActorInternal<ReturnType, ActorType>(
      actor_func,
      ActorExecFunction<ReturnType, ActorType, typename FilterArgType<Args>::type...>,
      *this, args...);
}

}  // namespace api
}  // namespace ray
