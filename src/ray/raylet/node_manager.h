#ifndef RAY_RAYLET_NODE_MANAGER_H
#define RAY_RAYLET_NODE_MANAGER_H

#include <boost/asio/steady_timer.hpp>

// clang-format off
#include "ray/rpc/grpc_client.h"
#include "ray/rpc/node_manager/node_manager_server.h"
#include "ray/rpc/node_manager/node_manager_client.h"
#include "ray/common/task/task.h"
#include "ray/common/ray_object.h"
#include "ray/common/client_connection.h"
#include "ray/common/task/task_common.h"
#include "ray/common/task/scheduling_resources.h"
#include "ray/common/scheduling/scheduling_ids.h"
#include "ray/common/scheduling/cluster_resource_scheduler.h"
#include "ray/object_manager/object_manager.h"
#include "ray/raylet/actor_registration.h"
#include "ray/raylet/lineage_cache.h"
#include "ray/raylet/scheduling_policy.h"
#include "ray/raylet/scheduling_queue.h"
#include "ray/raylet/reconstruction_policy.h"
#include "ray/raylet/task_dependency_manager.h"
#include "ray/raylet/worker_pool.h"
#include "ray/util/ordered_set.h"
// clang-format on

namespace ray {

namespace raylet {

using rpc::ActorTableData;
using rpc::ErrorType;
using rpc::GcsNodeInfo;
using rpc::HeartbeatBatchTableData;
using rpc::HeartbeatTableData;
using rpc::JobTableData;

struct NodeManagerConfig {
  /// The node's resource configuration.
  ResourceSet resource_config;
  /// The IP address this node manager is running on.
  std::string node_manager_address;
  /// The port to use for listening to incoming connections. If this is 0 then
  /// the node manager will choose its own port.
  int node_manager_port;
  /// The initial number of workers to create.
  int num_initial_workers;
  /// The maximum number of workers that can be started concurrently by a
  /// worker pool.
  int maximum_startup_concurrency;
  /// The commands used to start the worker process, grouped by language.
  WorkerCommandMap worker_commands;
  /// The time between heartbeats in milliseconds.
  uint64_t heartbeat_period_ms;
  /// The time between debug dumps in milliseconds, or -1 to disable.
  uint64_t debug_dump_period_ms;
  /// Whether to enable fair queueing between task classes in raylet.
  bool fair_queueing_enabled;
  /// Whether to enable pinning for plasma objects.
  bool object_pinning_enabled;
  /// the maximum lineage size.
  uint64_t max_lineage_size;
  /// The store socket name.
  std::string store_socket_name;
  /// The path to the ray temp dir.
  std::string temp_dir;
  /// The path of this ray session dir.
  std::string session_dir;
};

class NodeManager : public rpc::NodeManagerServiceHandler {
 public:
  /// Create a node manager.
  ///
  /// \param resource_config The initial set of node resources.
  /// \param object_manager A reference to the local object manager.
  NodeManager(boost::asio::io_service &io_service, const ClientID &self_node_id,
              const NodeManagerConfig &config, ObjectManager &object_manager,
              std::shared_ptr<gcs::GcsClient> gcs_client,
              std::shared_ptr<ObjectDirectoryInterface> object_directory_);

  /// Process a new client connection.
  ///
  /// \param client The client to process.
  /// \return Void.
  void ProcessNewClient(LocalClientConnection &client);

  /// Process a message from a client. This method is responsible for
  /// explicitly listening for more messages from the client if the client is
  /// still alive.
  ///
  /// \param client The client that sent the message.
  /// \param message_type The message type (e.g., a flatbuffer enum).
  /// \param message_data A pointer to the message data.
  /// \return Void.
  void ProcessClientMessage(const std::shared_ptr<LocalClientConnection> &client,
                            int64_t message_type, const uint8_t *message_data);

  /// Subscribe to the relevant GCS tables and set up handlers.
  ///
  /// \return Status indicating whether this was done successfully or not.
  ray::Status RegisterGcs();

  /// Get initial node manager configuration.
  const NodeManagerConfig &GetInitialConfig() const;

  /// Returns debug string for class.
  ///
  /// \return string.
  std::string DebugString() const;

  /// Record metrics.
  void RecordMetrics();

  /// Get the port of the node manager rpc server.
  int GetServerPort() const { return node_manager_server_.GetPort(); }

 private:
  /// Methods for handling clients.

  /// Handle an unexpected failure notification from GCS pubsub.
  ///
  /// \param worker_address The address of the worker that died.
  void HandleUnexpectedWorkerFailure(const rpc::Address &worker_address);

  /// Handler for the addition of a new node.
  ///
  /// \param data Data associated with the new node.
  /// \return Void.
  void NodeAdded(const GcsNodeInfo &data);

  /// Handler for the removal of a GCS node.
  /// \param node_info Data associated with the removed node.
  /// \return Void.
  void NodeRemoved(const GcsNodeInfo &node_info);

  /// Handler for the addition or updation of a resource in the GCS
  /// \param client_id ID of the node that created or updated resources.
  /// \param createUpdatedResources Created or updated resources.
  /// \return Void.
  void ResourceCreateUpdated(const ClientID &client_id,
                             const ResourceSet &createUpdatedResources);

  /// Handler for the deletion of a resource in the GCS
  /// \param client_id ID of the node that deleted resources.
  /// \param resource_names Names of deleted resources.
  /// \return Void.
  void ResourceDeleted(const ClientID &client_id,
                       const std::vector<std::string> &resource_names);

  /// Evaluates the local infeasible queue to check if any tasks can be scheduled.
  /// This is called whenever there's an update to the resources on the local client.
  /// \return Void.
  void TryLocalInfeasibleTaskScheduling();

  /// Send heartbeats to the GCS.
  void Heartbeat();

  /// Write out debug state to a file.
  void DumpDebugState() const;

  /// Get profiling information from the object manager and push it to the GCS.
  ///
  /// \return Void.
  void GetObjectManagerProfileInfo();

  /// Handler for a heartbeat notification from the GCS.
  ///
  /// \param id The ID of the node manager that sent the heartbeat.
  /// \param data The heartbeat data including load information.
  /// \return Void.
  void HeartbeatAdded(const ClientID &id, const HeartbeatTableData &data);
  /// Handler for a heartbeat batch notification from the GCS
  ///
  /// \param heartbeat_batch The batch of heartbeat data.
  void HeartbeatBatchAdded(const HeartbeatBatchTableData &heartbeat_batch);

  /// Methods for task scheduling.

  /// Enqueue a placeable task to wait on object dependencies or be ready for
  /// dispatch.
  ///
  /// \param task The task in question.
  /// \return Void.
  void EnqueuePlaceableTask(const Task &task);
  /// This will treat a task removed from the local queue as if it had been
  /// executed and failed. This is done by looping over the task return IDs and
  /// for each ID storing an object that represents a failure in the object
  /// store. When clients retrieve these objects, they will raise
  /// application-level exceptions. State for the task will be cleaned up as if
  /// it were any other task that had been assigned, executed, and removed from
  /// the local queue.
  ///
  /// \param task The task to fail.
  /// \param error_type The type of the error that caused this task to fail.
  /// \return Void.
  void TreatTaskAsFailed(const Task &task, const ErrorType &error_type);
  /// Mark the specified objects as failed with the given error type.
  ///
  /// \param error_type The type of the error that caused this task to fail.
  /// \param object_ids The object ids to store error messages into.
  /// \param job_id The optional job to push errors to if the writes fail.
  void MarkObjectsAsFailed(const ErrorType &error_type,
                           const std::vector<plasma::ObjectID> object_ids,
                           const JobID &job_id);
  /// This is similar to TreatTaskAsFailed, but it will only mark the task as
  /// failed if at least one of the task's return values is lost. A return
  /// value is lost if it has been created before, but no longer exists on any
  /// nodes, due to either node failure or eviction.
  ///
  /// \param task The task to potentially fail.
  /// \return Void.
  void TreatTaskAsFailedIfLost(const Task &task);
  /// Handle specified task's submission to the local node manager.
  ///
  /// \param task The task being submitted.
  /// \param uncommitted_lineage The uncommitted lineage of the task.
  /// \param forwarded True if the task has been forwarded from a different
  /// node manager and false if it was submitted by a local worker.
  /// \return Void.
  void SubmitTask(const Task &task, const Lineage &uncommitted_lineage,
                  bool forwarded = false);
  /// Assign a task to a worker. The task is assumed to not be queued in local_queues_.
  ///
  /// \param[in] worker The worker to assign the task to.
  /// \param[in] task The task in question.
  /// \param[out] post_assign_callbacks Vector of callbacks that will be appended
  /// to with any logic that should run after the DispatchTasks loop runs.
  void AssignTask(const std::shared_ptr<Worker> &worker, const Task &task,
                  std::vector<std::function<void()>> *post_assign_callbacks);
  /// Handle a worker finishing its assigned task.
  ///
  /// \param worker The worker that finished the task.
  /// \return Whether the worker should be returned to the idle pool. This is
  /// only false for direct actor creation calls, which should never be
  /// returned to idle.
  bool FinishAssignedTask(Worker &worker);
  /// Helper function to produce actor table data for a newly created actor.
  ///
  /// \param task_spec Task specification of the actor creation task that created the
  /// actor.
  /// \param worker The port that the actor is listening on.
  std::shared_ptr<ActorTableData> CreateActorTableDataFromCreationTask(
      const TaskSpecification &task_spec, int port, const WorkerID &worker_id);
  /// Handle a worker finishing an assigned actor task or actor creation task.
  /// \param worker The worker that finished the task.
  /// \param task The actor task or actor creation task.
  /// \return Void.
  void FinishAssignedActorTask(Worker &worker, const Task &task);
  /// Helper function for handling worker to finish its assigned actor task
  /// or actor creation task. Gets invoked when tasks's parent actor is known.
  ///
  /// \param parent_actor_id The actor id corresponding to the actor which creates
  /// the new actor.
  /// \param task_spec Task specification of the actor creation task that created the
  /// actor.
  /// \param resumed_from_checkpoint If the actor was resumed from a checkpoint.
  /// \param port Rpc server port that the actor is listening on.
  /// \return Void.
  void FinishAssignedActorCreationTask(const ActorID &parent_actor_id,
                                       const TaskSpecification &task_spec,
                                       bool resumed_from_checkpoint, int port,
                                       const WorkerID &worker_id);
  /// Make a placement decision for placeable tasks given the resource_map
  /// provided. This will perform task state transitions and task forwarding.
  ///
  /// \param resource_map A mapping from node manager ID to an estimate of the
  /// resources available to that node manager. Scheduling decisions will only
  /// consider the local node manager and the node managers in the keys of the
  /// resource_map argument.
  /// \return Void.
  void ScheduleTasks(std::unordered_map<ClientID, SchedulingResources> &resource_map);
  /// Handle a task whose return value(s) must be reconstructed.
  ///
  /// \param task_id The relevant task ID.
  /// \param required_object_id The object id we are reconstructing for.
  /// \return Void.
  void HandleTaskReconstruction(const TaskID &task_id,
                                const ObjectID &required_object_id);
  /// Resubmit a task for execution. This is a task that was previously already
  /// submitted to a raylet but which must now be re-executed.
  ///
  /// \param task The task being resubmitted.
  /// \param required_object_id The object id that triggered the resubmission.
  /// \return Void.
  void ResubmitTask(const Task &task, const ObjectID &required_object_id);
  /// Attempt to forward a task to a remote different node manager. If this
  /// fails, the task will be resubmit locally.
  ///
  /// \param task The task in question.
  /// \param node_manager_id The ID of the remote node manager.
  /// \return Void.
  void ForwardTaskOrResubmit(const Task &task, const ClientID &node_manager_id);
  /// Forward a task to another node to execute. The task is assumed to not be
  /// queued in local_queues_.
  ///
  /// \param task The task to forward.
  /// \param node_id The ID of the node to forward the task to.
  /// \param on_error Callback on run on non-ok status.
  void ForwardTask(
      const Task &task, const ClientID &node_id,
      const std::function<void(const ray::Status &, const Task &)> &on_error);

  /// Dispatch locally scheduled tasks. This attempts the transition from "scheduled" to
  /// "running" task state.
  ///
  /// This function is called in the following cases:
  ///   (1) A set of new tasks is added to the ready queue.
  ///   (2) New resources are becoming available on the local node.
  ///   (3) A new worker becomes available.
  /// Note in case (1) we only need to look at the new tasks added to the
  /// ready queue, as we know that the old tasks in the ready queue cannot
  /// be scheduled (We checked those tasks last time new resources or
  /// workers became available, and nothing changed since then.) In this case,
  /// tasks_with_resources contains only the newly added tasks to the
  /// ready queue. Otherwise, tasks_with_resources points to entire ready queue.
  /// \param tasks_with_resources Mapping from resource shapes to tasks with
  /// that resource shape.
  void DispatchTasks(
      const std::unordered_map<SchedulingClass, ordered_set<TaskID>> &tasks_by_class);

  /// Handle blocking gets of objects. This could be a task assigned to a worker,
  /// an out-of-band task (e.g., a thread created by the application), or a
  /// driver task. This can be triggered when a client starts a get call or a
  /// wait call.
  ///
  /// \param client The client that is executing the blocked task.
  /// \param required_object_ids The IDs that the client is blocked waiting for.
  /// \param current_task_id The task that is blocked.
  /// \param ray_get Whether the task is blocked in a `ray.get` call.
  /// \param mark_worker_blocked Whether to mark the worker as blocked. This
  ///                            should be False for direct calls.
  /// \return Void.
  void AsyncResolveObjects(const std::shared_ptr<LocalClientConnection> &client,
                           const std::vector<ObjectID> &required_object_ids,
                           const TaskID &current_task_id, bool ray_get,
                           bool mark_worker_blocked);

  /// Handle end of a blocking object get. This could be a task assigned to a
  /// worker, an out-of-band task (e.g., a thread created by the application),
  /// or a driver task. This can be triggered when a client finishes a get call
  /// or a wait call. The given task must be blocked, via a previous call to
  /// AsyncResolveObjects.
  ///
  /// \param client The client that is executing the unblocked task.
  /// \param current_task_id The task that is unblocked.
  /// \param worker_was_blocked Whether we previously marked the worker as
  ///                           blocked in AsyncResolveObjects().
  /// \return Void.
  void AsyncResolveObjectsFinish(const std::shared_ptr<LocalClientConnection> &client,
                                 const TaskID &current_task_id, bool was_blocked);

  /// Handle a direct call task that is blocked. Note that this callback may
  /// arrive after the worker lease has been returned to the node manager.
  ///
  /// \param worker Shared ptr to the worker, or nullptr if lost.
  void HandleDirectCallTaskBlocked(const std::shared_ptr<Worker> &worker);

  /// Handle a direct call task that is unblocked. Note that this callback may
  /// arrive after the worker lease has been returned to the node manager.
  /// However, it is guaranteed to arrive after DirectCallTaskBlocked.
  ///
  /// \param worker Shared ptr to the worker, or nullptr if lost.
  void HandleDirectCallTaskUnblocked(const std::shared_ptr<Worker> &worker);

  /// Kill a worker.
  ///
  /// \param worker The worker to kill.
  /// \return Void.
  void KillWorker(std::shared_ptr<Worker> worker);

  /// The callback for handling an actor state transition (e.g., from ALIVE to
  /// DEAD), whether as a notification from the actor table or as a handler for
  /// a local actor's state transition. This method is idempotent and will ignore
  /// old state transition.
  ///
  /// \param actor_id The actor ID of the actor whose state was updated.
  /// \param actor_registration The ActorRegistration object that represents actor's
  /// new state.
  /// \return Void.
  void HandleActorStateTransition(const ActorID &actor_id,
                                  ActorRegistration &&actor_registration);

  /// When a job finished, loop over all of the queued tasks for that job and
  /// treat them as failed.
  ///
  /// \param job_id The job that exited.
  /// \return Void.
  void CleanUpTasksForFinishedJob(const JobID &job_id);

  /// Handle an object becoming local. This updates any local accounting, but
  /// does not write to any global accounting in the GCS.
  ///
  /// \param object_id The object that is locally available.
  /// \return Void.
  void HandleObjectLocal(const ObjectID &object_id);
  /// Handle an object that is no longer local. This updates any local
  /// accounting, but does not write to any global accounting in the GCS.
  ///
  /// \param object_id The object that has been evicted locally.
  /// \return Void.
  void HandleObjectMissing(const ObjectID &object_id);

  /// Handles the event that a job is finished.
  ///
  /// \param job_id ID of the finished job.
  /// \param job_data Data associated with the finished job.
  /// \return Void.
  void HandleJobFinished(const JobID &job_id, const JobTableData &job_data);

  /// Check if certain invariants associated with the task dependency manager
  /// and the local queues are satisfied. This is only used for debugging
  /// purposes.
  ///
  /// \return True if the invariants are satisfied and false otherwise.
  bool CheckDependencyManagerInvariant() const;

  /// Process client message of SubmitTask
  ///
  /// \param message_data A pointer to the message data.
  /// \return Void.
  void ProcessSubmitTaskMessage(const uint8_t *message_data);

  /// Process client message of RegisterClientRequest
  ///
  /// \param client The client that sent the message.
  /// \param message_data A pointer to the message data.
  /// \return Void.
  void ProcessRegisterClientRequestMessage(
      const std::shared_ptr<LocalClientConnection> &client, const uint8_t *message_data);

  /// Handle the case that a worker is available.
  ///
  /// \param client The connection for the worker.
  /// \return Void.
  void HandleWorkerAvailable(const std::shared_ptr<LocalClientConnection> &client);

  /// Handle the case that a worker is available.
  ///
  /// \param worker The pointer to the worker
  /// \return Void.
  void HandleWorkerAvailable(const std::shared_ptr<Worker> &worker);

  /// Handle a client that has disconnected. This can be called multiple times
  /// on the same client because this is triggered both when a client
  /// disconnects and when the node manager fails to write a message to the
  /// client.
  ///
  /// \param client The client that sent the message.
  /// \param intentional_disconnect Whether the client was intentionally disconnected.
  /// \return Void.
  void ProcessDisconnectClientMessage(
      const std::shared_ptr<LocalClientConnection> &client,
      bool intentional_disconnect = false);

  /// Process client message of FetchOrReconstruct
  ///
  /// \param client The client that sent the message.
  /// \param message_data A pointer to the message data.
  /// \return Void.
  void ProcessFetchOrReconstructMessage(
      const std::shared_ptr<LocalClientConnection> &client, const uint8_t *message_data);

  /// Process client message of WaitRequest
  ///
  /// \param client The client that sent the message.
  /// \param message_data A pointer to the message data.
  /// \return Void.
  void ProcessWaitRequestMessage(const std::shared_ptr<LocalClientConnection> &client,
                                 const uint8_t *message_data);

  /// Process client message of WaitForDirectActorCallArgsRequest
  ///
  /// \param client The client that sent the message.
  /// \param message_data A pointer to the message data.
  /// \return Void.
  void ProcessWaitForDirectActorCallArgsRequestMessage(
      const std::shared_ptr<LocalClientConnection> &client, const uint8_t *message_data);

  /// Process client message of PushErrorRequest
  ///
  /// \param message_data A pointer to the message data.
  /// \return Void.
  void ProcessPushErrorRequestMessage(const uint8_t *message_data);

  /// Process client message of PrepareActorCheckpointRequest.
  ///
  /// \param client The client that sent the message.
  /// \param message_data A pointer to the message data.
  void ProcessPrepareActorCheckpointRequest(
      const std::shared_ptr<LocalClientConnection> &client, const uint8_t *message_data);

  /// Process client message of NotifyActorResumedFromCheckpoint.
  ///
  /// \param message_data A pointer to the message data.
  void ProcessNotifyActorResumedFromCheckpoint(const uint8_t *message_data);

  /// Process client message of ReportActiveObjectIDs.
  ///
  /// \param client The client that sent the message.
  /// \param message_data A pointer to the message data.
  void ProcessReportActiveObjectIDs(const std::shared_ptr<LocalClientConnection> &client,
                                    const uint8_t *message_data);

  /// Update actor frontier when a task finishes.
  /// If the task is an actor creation task and the actor was resumed from a checkpoint,
  /// restore the frontier from the checkpoint. Otherwise, just extend actor frontier.
  ///
  /// \param task The task that just finished.
  void UpdateActorFrontier(const Task &task);

  /// Process client message of SetResourceRequest
  /// \param client The client that sent the message.
  /// \param message_data A pointer to the message data.
  /// \return Void.
  void ProcessSetResourceRequest(const std::shared_ptr<LocalClientConnection> &client,
                                 const uint8_t *message_data);

  /// Handle the case where an actor is disconnected, determine whether this
  /// actor needs to be reconstructed and then update actor table.
  /// This function needs to be called either when actor process dies or when
  /// a node dies.
  ///
  /// \param actor_id Id of this actor.
  /// \param was_local Whether the disconnected was on this local node.
  /// \param intentional_disconnect Wether the client was intentionally disconnected.
  /// \return Void.
  void HandleDisconnectedActor(const ActorID &actor_id, bool was_local,
                               bool intentional_disconnect);

  /// Finish assigning a task to a worker.
  ///
  /// \param worker Worker that the task is assigned to.
  /// \param task_id Id of the task.
  /// \param success Whether or not assigning the task was successful.
  /// \return void.
  void FinishAssignTask(const std::shared_ptr<Worker> &worker, const TaskID &task_id,
                        bool success);

  /// Handle a `WorkerLease` request.
  void HandleRequestWorkerLease(const rpc::RequestWorkerLeaseRequest &request,
                                rpc::RequestWorkerLeaseReply *reply,
                                rpc::SendReplyCallback send_reply_callback) override;

  /// Handle a `ReturnWorker` request.
  void HandleReturnWorker(const rpc::ReturnWorkerRequest &request,
                          rpc::ReturnWorkerReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override;

  /// Handle a `ForwardTask` request.
  void HandleForwardTask(const rpc::ForwardTaskRequest &request,
                         rpc::ForwardTaskReply *reply,
                         rpc::SendReplyCallback send_reply_callback) override;

  /// Handle a `PinObjectIDs` request.
  void HandlePinObjectIDs(const rpc::PinObjectIDsRequest &request,
                          rpc::PinObjectIDsReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override;

  /// Handle a `NodeStats` request.
  void HandleGetNodeStats(const rpc::GetNodeStatsRequest &request,
                          rpc::GetNodeStatsReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override;

  /// Push an error to the driver if this node is full of actors and so we are
  /// unable to schedule new tasks or actors at all.
  void WarnResourceDeadlock();

  /// Dispatch tasks to available workers.
  void DispatchScheduledTasksToWorkers();

  /// For the pending task at the head of tasks_to_schedule_, return a node
  /// in the system (local or remote) that has enough resources available to
  /// run the task, if any such node exist.
  /// Repeat the process as long as we can schedule a task.
  void NewSchedulerSchedulePendingTasks();

  /// Whether a task is an direct actor creation task.
  bool IsDirectActorCreationTask(const TaskID &task_id);

  /// ID of this node.
  ClientID self_node_id_;
  boost::asio::io_service &io_service_;
  ObjectManager &object_manager_;
  /// A Plasma object store client. This is used for creating new objects in
  /// the object store (e.g., for actor tasks that can't be run because the
  /// actor died) and to pin objects that are in scope in the cluster.
  plasma::PlasmaClient store_client_;
  /// A client connection to the GCS.
  std::shared_ptr<gcs::GcsClient> gcs_client_;
  /// The object table. This is shared with the object manager.
  std::shared_ptr<ObjectDirectoryInterface> object_directory_;
  /// The timer used to send heartbeats.
  boost::asio::steady_timer heartbeat_timer_;
  /// The period used for the heartbeat timer.
  std::chrono::milliseconds heartbeat_period_;
  /// The period between debug state dumps.
  int64_t debug_dump_period_;
  /// Whether to enable fair queueing between task classes in raylet.
  bool fair_queueing_enabled_;
  /// Whether to enable pinning for plasma objects.
  bool object_pinning_enabled_;
  /// Whether we have printed out a resource deadlock warning.
  bool resource_deadlock_warned_ = false;
  /// Whether we have recorded any metrics yet.
  bool recorded_metrics_ = false;
  /// The path to the ray temp dir.
  std::string temp_dir_;
  /// The timer used to get profiling information from the object manager and
  /// push it to the GCS.
  boost::asio::steady_timer object_manager_profile_timer_;
  /// The time that the last heartbeat was sent at. Used to make sure we are
  /// keeping up with heartbeats.
  uint64_t last_heartbeat_at_ms_;
  /// The time that the last debug string was logged to the console.
  uint64_t last_debug_dump_at_ms_;
  /// Initial node manager configuration.
  const NodeManagerConfig initial_config_;
  /// The resources (and specific resource IDs) that are currently available.
  ResourceIdSet local_available_resources_;
  std::unordered_map<ClientID, SchedulingResources> cluster_resource_map_;
  /// A pool of workers.
  WorkerPool worker_pool_;
  /// A set of queues to maintain tasks.
  SchedulingQueue local_queues_;
  /// The scheduling policy in effect for this raylet.
  SchedulingPolicy scheduling_policy_;
  /// The reconstruction policy for deciding when to re-execute a task.
  ReconstructionPolicy reconstruction_policy_;
  /// A manager to make waiting tasks's missing object dependencies available.
  TaskDependencyManager task_dependency_manager_;
  /// The lineage cache for the GCS object and task tables.
  LineageCache lineage_cache_;
  /// A mapping from actor ID to registration information about that actor
  /// (including which node manager owns it).
  std::unordered_map<ActorID, ActorRegistration> actor_registry_;

  /// This map stores actor ID to the ID of the checkpoint that will be used to
  /// restore the actor.
  std::unordered_map<ActorID, ActorCheckpointID> checkpoint_id_to_restore_;

  /// The RPC server.
  rpc::GrpcServer node_manager_server_;

  /// The node manager RPC service.
  rpc::NodeManagerGrpcService node_manager_service_;

  /// The `ClientCallManager` object that is shared by all `NodeManagerClient`s
  /// as well as all `CoreWorkerClient`s.
  rpc::ClientCallManager client_call_manager_;

  /// Map from node ids to clients of the remote node managers.
  std::unordered_map<ClientID, std::unique_ptr<rpc::NodeManagerClient>>
      remote_node_manager_clients_;

  /// Map of workers leased out to direct call clients.
  std::unordered_map<WorkerID, std::shared_ptr<Worker>> leased_workers_;

  /// Map from owner worker ID to a list of worker IDs that the owner has a
  /// lease on.
  absl::flat_hash_map<WorkerID, std::vector<WorkerID>> leased_workers_by_owner_;

  /// Whether new schedule is enabled.
  const bool new_scheduler_enabled_;

  /// The new resource scheduler for direct task calls.
  std::shared_ptr<ClusterResourceScheduler> new_resource_scheduler_;
  /// Map of leased workers to their current resource usage.
  /// TODO(ion): Check whether we can track these resources in the worker.
  std::unordered_map<WorkerID, ResourceSet> leased_worker_resources_;

  typedef std::function<void(std::shared_ptr<Worker>, ClientID spillback_to,
                             std::string address, int port)>
      ScheduleFn;

  /// Queue of lease requests that are waiting for resources to become available.
  /// TODO this should be a queue for each SchedulingClass
  std::deque<std::pair<ScheduleFn, Task>> tasks_to_schedule_;
  /// Queue of lease requests that should be scheduled onto workers.
  std::deque<std::pair<ScheduleFn, Task>> tasks_to_dispatch_;

  /// Cache of gRPC clients to workers (not necessarily running on this node).
  /// Also includes the number of inflight requests to each worker - when this
  /// reaches zero, the client will be deleted and a new one will need to be created
  /// for any subsequent requests.
  absl::flat_hash_map<WorkerID, std::pair<std::unique_ptr<rpc::CoreWorkerClient>, size_t>>
      worker_rpc_clients_;

  absl::flat_hash_map<ObjectID, std::unique_ptr<RayObject>> pinned_objects_;

  /// Wait for a task's arguments to become ready.
  void WaitForTaskArgsRequests(std::pair<ScheduleFn, Task> &work);

  // TODO(swang): Evict entries from these caches.
  /// Cache for the WorkerFailureTable in the GCS.
  absl::flat_hash_set<WorkerID> failed_workers_cache_;
  /// Cache for the ClientTable in the GCS.
  absl::flat_hash_set<ClientID> failed_nodes_cache_;
};

}  // namespace raylet

}  // end namespace ray

#endif  // RAY_RAYLET_NODE_MANAGER_H
