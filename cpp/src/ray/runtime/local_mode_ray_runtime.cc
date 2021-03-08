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


#include "local_mode_ray_runtime.h"

#include <ray/api.h>

#include "../util/address_helper.h"
#include "./object/local_mode_object_store.h"
#include "./object/object_store.h"
#include "./task/local_mode_task_submitter.h"

namespace ray {
namespace api {

LocalModeRayRuntime::LocalModeRayRuntime(std::shared_ptr<RayConfig> config) {
  config_ = config;
  worker_ = std::unique_ptr<WorkerContext>(new WorkerContext(
      WorkerType::DRIVER, ComputeDriverIdFromJob(JobID::Nil()), JobID::Nil()));
  object_store_ = std::unique_ptr<ObjectStore>(new LocalModeObjectStore(*this));
  task_submitter_ = std::unique_ptr<TaskSubmitter>(new LocalModeTaskSubmitter(*this));
}

ActorID LocalModeRayRuntime::GetNextActorID() {
  const auto next_task_index = worker_->GetNextTaskIndex();
  const ActorID actor_id = ActorID::Of(worker_->GetCurrentJobID(),
                                       worker_->GetCurrentTaskID(), next_task_index);
  return actor_id;
}

}  // namespace api
}  // namespace ray