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

#include "ray/raylet/worker.h"
#include "ray/rpc/server_call.h"
#include "src/ray/protobuf/node_manager.pb.h"

namespace ray {
namespace raylet {
class ClusterTaskManagerInterface {
 public:
  virtual ~ClusterTaskManagerInterface() = default;

  /// Return the resources that were being used by this worker.
  virtual void FreeLocalTaskResources(std::shared_ptr<WorkerInterface> worker) = 0;

  /// When direct call task is blocked, the worker who is executing the task should give
  /// up the cpu resources allocated for the running task for the time being and the
  /// worker itself should also be marked as blocked.
  ///
  /// \param worker The worker to be marked as blocked.
  /// \return true if the worker is non-block and release_resources is true, else false.
  virtual bool ReleaseCpuResourcesAndMarkWorkerAsBlocked(
      std::shared_ptr<WorkerInterface> worker, bool release_resources) = 0;

  /// When direct call task is unblocked, the cpu resources that the worker gave up should
  /// be returned to it.
  /// \param worker The blocked worker.
  /// \return true if the worker is blocking, else false.
  virtual bool ReturnCpuResourcesAndMarkWorkerAsUnblocked(
      std::shared_ptr<WorkerInterface> worker) = 0;

  // Schedule and dispatch tasks.
  virtual void ScheduleAndDispatchTasks() = 0;

  /// Move tasks from waiting to ready for dispatch. Called when a task's
  /// dependencies are resolved.
  ///
  /// \param readyIds: The tasks which are now ready to be dispatched.
  virtual void TasksUnblocked(const std::vector<TaskID> &ready_ids) = 0;

  /// Populate the relevant parts of the heartbeat table. This is intended for
  /// sending raylet <-> gcs heartbeats. In particular, this should fill in
  /// resource_load and resource_load_by_shape.
  ///
  /// \param Output parameter. `resource_load` and `resource_load_by_shape` are the only
  /// fields used.
  virtual void FillResourceUsage(std::shared_ptr<rpc::ResourcesData> data) = 0;

  /// Populate the list of pending or infeasible actor tasks for node stats.
  ///
  /// \param Output parameter.
  virtual void FillPendingActorInfo(rpc::GetNodeStatsReply *reply) const = 0;

  /// Call once a task finishes (i.e. a worker is returned).
  ///
  /// \param worker: The worker which was running the task.
  /// \param finished_task The finished task that will be filled if it is not null.
  virtual void HandleTaskFinished(std::shared_ptr<WorkerInterface> worker,
                                  Task *finished_task = nullptr) = 0;

  /// Return worker resources.
  ///
  /// \param worker: The worker which was running the task.
  virtual void ReturnWorkerResources(std::shared_ptr<WorkerInterface> worker) = 0;

  /// Attempt to cancel an already queued task.
  ///
  /// \param task_id: The id of the task to remove.
  ///
  /// \return True if task was successfully removed. This function will return
  /// false if the task is already running.
  virtual bool CancelTask(const TaskID &task_id) = 0;

  /// Queue task and schedule. This hanppens when processing the worker lease request.
  /// \param fn: The function used during dispatching.
  /// \param task: The incoming task to schedule.
  virtual void QueueAndScheduleTask(Task &&task, rpc::RequestWorkerLeaseReply *reply,
                                    rpc::SendReplyCallback send_reply_callback) = 0;

  /// Return if any tasks are pending resource acquisition.
  ///
  /// \param[in] exemplar An example task that is deadlocking.
  /// \param[in] num_pending_actor_creation Number of pending actor creation tasks.
  /// \param[in] num_pending_tasks Number of pending tasks.
  /// \param[in] any_pending True if there's any pending exemplar.
  /// \return True if any progress is any tasks are pending.
  virtual bool AnyPendingTasks(Task *exemplar, bool *any_pending,
                               int *num_pending_actor_creation,
                               int *num_pending_tasks) const = 0;

  /// Handle the resource usage updated event of the specified node.
  ///
  /// \param node_id ID of the node which resources are updated.
  /// \param resource_data The node resources.
  virtual void OnNodeResourceUsageUpdated(const NodeID &node_id,
                                          const rpc::ResourcesData &resource_data) = 0;

  /// Handle the bundle resources prepared event.
  virtual void OnBundleResourcesPrepared() = 0;

  /// Handle the bundle resources committed event.
  virtual void OnBundleResourcesCommitted() = 0;

  /// Handle the reserved resources canceled event.
  virtual void OnReservedResourcesCanceled() = 0;

  /// Handle the object missing event.
  virtual void OnObjectMissing(const ObjectID &object_id,
                               const std::vector<TaskID> &waiting_task_ids) = 0;

  /// The helper to dump the debug state of the cluster task manater.
  ///
  /// As the NodeManager inherites from ClusterTaskManager and the
  /// `cluster_task_manager_->DebugString()` is invoked inside
  /// `NodeManager::DebugString()`, which will leads to infinite loop and cause stack
  /// overflow, so we should rename `DebugString` to `DebugStr` to avoid this.
  virtual std::string DebugStr() const = 0;
};
}  // namespace raylet
}  // namespace ray
