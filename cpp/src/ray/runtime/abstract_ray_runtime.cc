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

#include "abstract_ray_runtime.h"

#include <ray/api.h>
#include <ray/api/ray_config.h>
#include <ray/api/ray_exception.h>

#include <cassert>

#include "../util/address_helper.h"
#include "../util/process_helper.h"
#include "local_mode_ray_runtime.h"
#include "native_ray_runtime.h"

namespace ray {
namespace api {
std::shared_ptr<AbstractRayRuntime> AbstractRayRuntime::abstract_ray_runtime_ = nullptr;

std::shared_ptr<AbstractRayRuntime> AbstractRayRuntime::DoInit(
    std::shared_ptr<RayConfig> config) {
  std::shared_ptr<AbstractRayRuntime> runtime;
  if (config->run_mode == RunMode::SINGLE_PROCESS) {
    runtime = std::shared_ptr<AbstractRayRuntime>(new LocalModeRayRuntime(config));
  } else {
    ProcessHelper::GetInstance().RayStart(config, TaskExecutor::ExecuteTask);
    runtime = std::shared_ptr<AbstractRayRuntime>(new NativeRayRuntime(config));
  }
  runtime->config_ = config;
  RAY_CHECK(runtime);
  abstract_ray_runtime_ = runtime;
  return runtime;
}

std::shared_ptr<AbstractRayRuntime> AbstractRayRuntime::GetInstance() {
  return abstract_ray_runtime_;
}

void AbstractRayRuntime::DoShutdown(std::shared_ptr<RayConfig> config) {
  if (config->run_mode == RunMode::CLUSTER) {
    ProcessHelper::GetInstance().RayStop(config);
  }
}

void AbstractRayRuntime::Put(std::shared_ptr<msgpack::sbuffer> data,
                             ObjectID *object_id) {
  object_store_->Put(data, object_id);
}

void AbstractRayRuntime::Put(std::shared_ptr<msgpack::sbuffer> data,
                             const ObjectID &object_id) {
  object_store_->Put(data, object_id);
}

ObjectID AbstractRayRuntime::Put(std::shared_ptr<msgpack::sbuffer> data) {
  ObjectID object_id =
      ObjectID::FromIndex(worker_->GetCurrentTaskID(), worker_->GetNextPutIndex());
  Put(data, &object_id);
  return object_id;
}

std::shared_ptr<msgpack::sbuffer> AbstractRayRuntime::Get(const ObjectID &object_id) {
  return object_store_->Get(object_id, -1);
}

std::vector<std::shared_ptr<msgpack::sbuffer>> AbstractRayRuntime::Get(
    const std::vector<ObjectID> &ids) {
  return object_store_->Get(ids, -1);
}

WaitResult AbstractRayRuntime::Wait(const std::vector<ObjectID> &ids, int num_objects,
                                    int timeout_ms) {
  return object_store_->Wait(ids, num_objects, timeout_ms);
}

InvocationSpec BuildInvocationSpec(TaskType task_type, std::string lib_name,
                                   const RemoteFunctionPtrHolder &fptr,
                                   std::vector<std::unique_ptr<::ray::TaskArg>> &args,
                                   const ActorID &actor) {
  InvocationSpec invocation_spec;
  invocation_spec.task_type = task_type;
  invocation_spec.task_id =
      TaskID::ForFakeTask();  // TODO(Guyang Song): make it from different task
  invocation_spec.lib_name = lib_name;
  invocation_spec.fptr = fptr;
  invocation_spec.actor_id = actor;
  invocation_spec.args = std::move(args);
  return invocation_spec;
}

ObjectID AbstractRayRuntime::Call(const RemoteFunctionPtrHolder &fptr,
                                  std::vector<std::unique_ptr<::ray::TaskArg>> &args) {
  auto invocation_spec = BuildInvocationSpec(
      TaskType::NORMAL_TASK, this->config_->lib_name, fptr, args, ActorID::Nil());
  return task_submitter_->SubmitTask(invocation_spec);
}

ActorID AbstractRayRuntime::CreateActor(
    const RemoteFunctionPtrHolder &fptr,
    std::vector<std::unique_ptr<::ray::TaskArg>> &args) {
  auto invocation_spec = BuildInvocationSpec(
      TaskType::ACTOR_CREATION_TASK, this->config_->lib_name, fptr, args, ActorID::Nil());
  return task_submitter_->CreateActor(invocation_spec);
}

ObjectID AbstractRayRuntime::CallActor(
    const RemoteFunctionPtrHolder &fptr, const ActorID &actor,
    std::vector<std::unique_ptr<::ray::TaskArg>> &args) {
  auto invocation_spec = BuildInvocationSpec(TaskType::ACTOR_TASK,
                                             this->config_->lib_name, fptr, args, actor);
  return task_submitter_->SubmitActorTask(invocation_spec);
}

const TaskID &AbstractRayRuntime::GetCurrentTaskId() {
  return worker_->GetCurrentTaskID();
}

const JobID &AbstractRayRuntime::GetCurrentJobID() { return worker_->GetCurrentJobID(); }

const std::unique_ptr<WorkerContext> &AbstractRayRuntime::GetWorkerContext() {
  return worker_;
}

}  // namespace api
}  // namespace ray
