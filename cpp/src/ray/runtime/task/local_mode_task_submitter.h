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

#include <boost/asio/thread_pool.hpp>
#include <memory>
#include <queue>

#include "../local_mode_ray_runtime.h"
#include "absl/synchronization/mutex.h"
#include "invocation_spec.h"
#include "ray/core.h"
#include "task_executor.h"
#include "task_submitter.h"

namespace ray {
namespace api {

class LocalModeTaskSubmitter : public TaskSubmitter {
 public:
  LocalModeTaskSubmitter(LocalModeRayRuntime &local_mode_ray_tuntime);

  ObjectID SubmitTask(InvocationSpec &invocation);

  ActorID CreateActor(InvocationSpec &invocation);

  ObjectID SubmitActorTask(InvocationSpec &invocation);

 private:
  std::unordered_map<ActorID, std::unique_ptr<ActorContext>> actor_contexts_;

  absl::Mutex actor_contexts_mutex_;

  std::unique_ptr<boost::asio::thread_pool> thread_pool_;

  LocalModeRayRuntime &local_mode_ray_tuntime_;

  ObjectID Submit(InvocationSpec &invocation);
};
}  // namespace api
}  // namespace ray