#ifndef RAY_CORE_WORKER_CORE_WORKER_H
#define RAY_CORE_WORKER_CORE_WORKER_H

#include "ray/common/buffer.h"
#include "ray/core_worker/actor_handle.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/object_interface.h"
#include "ray/core_worker/profiling.h"
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/core_worker/transport/transport.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/raylet/raylet_client.h"
#include "ray/rpc/worker/worker_client.h"
#include "ray/rpc/worker/worker_server.h"

namespace ray {

/// The root class that contains all the core and language-independent functionalities
/// of the worker. This class is supposed to be used to implement app-language (Java,
/// Python, etc) workers.
class CoreWorker {
  // Callback that must be implemented and provided by the language-specific worker
  // frontend to execute tasks and return their results.
  using TaskExecutionCallback = std::function<Status(
      TaskType task_type, const RayFunction &ray_function,
      const std::unordered_map<std::string, double> &required_resources,
      const std::vector<std::shared_ptr<RayObject>> &args,
      const std::vector<ObjectID> &arg_reference_ids,
      const std::vector<ObjectID> &return_ids,
      std::vector<std::shared_ptr<RayObject>> *results)>;

 public:
  /// Construct a CoreWorker instance.
  ///
  /// \param[in] worker_type Type of this worker.
  /// \param[in] language Language of this worker.
  /// \param[in] store_socket Object store socket to connect to.
  /// \param[in] raylet_socket Raylet socket to connect to.
  /// \param[in] job_id Job ID of this worker.
  /// \param[in] gcs_options Options for the GCS client.
  /// \param[in] log_dir Directory to write logs to. If this is empty, logs
  ///            won't be written to a file.
  /// \param[in] node_ip_address IP address of the node.
  /// \param[in] task_execution_callback Language worker callback to execute tasks.
  /// \parma[in] check_signals Language worker function to check for signals and handle
  ///            them. If the function returns anything but StatusOK, any long-running
  ///            operations in the core worker will short circuit and return that status.
  /// \param[in] use_memory_store Whether or not to use the in-memory object store
  ///            in addition to the plasma store.
  ///
  /// NOTE(zhijunfu): the constructor would throw if a failure happens.
  /// NOTE(edoakes): the use_memory_store flag is a stop-gap solution to the issue
  ///                that randomly generated ObjectIDs may use the memory store
  ///                instead of the plasma store.
  CoreWorker(const WorkerType worker_type, const Language language,
             const std::string &store_socket, const std::string &raylet_socket,
             const JobID &job_id, const gcs::GcsClientOptions &gcs_options,
             const std::string &log_dir, const std::string &node_ip_address,
             const TaskExecutionCallback &task_execution_callback,
             std::function<Status()> check_signals = nullptr,
             bool use_memory_store = true);

  ~CoreWorker();

  void Disconnect();

  /// Type of this worker.
  WorkerType GetWorkerType() const { return worker_type_; }

  /// Language of this worker.
  Language GetLanguage() const { return language_; }

  WorkerContext &GetWorkerContext() { return worker_context_; }

  RayletClient &GetRayletClient() { return *raylet_client_; }

  /// Return the `CoreWorkerObjectInterface` that contains methods related to object
  /// store.
  CoreWorkerObjectInterface &Objects() { return object_interface_; }

  /// Create a profile event with a reference to the core worker's profiler.
  std::unique_ptr<worker::ProfileEvent> CreateProfileEvent(const std::string &event_type);

  // Get the resource IDs available to this worker (as assigned by the raylet).
  const ResourceMappingType GetResourceIDs() const;

  const TaskID &GetCurrentTaskId() const { return worker_context_.GetCurrentTaskID(); }

  void SetCurrentTaskId(const TaskID &task_id);

  const JobID &GetCurrentJobId() const { return worker_context_.GetCurrentJobID(); }

  void SetActorId(const ActorID &actor_id) {
    RAY_CHECK(actor_id_.IsNil());
    actor_id_ = actor_id;
  }

  const ActorID &GetActorId() const { return actor_id_; }

  /// Get the caller ID used to submit tasks from this worker to an actor.
  ///
  /// \return The caller ID. For non-actor tasks, this is the current task ID.
  /// For actors, this is the current actor ID. To make sure that all caller
  /// IDs have the same type, we embed the actor ID in a TaskID with the rest
  /// of the bytes zeroed out.
  TaskID GetCallerId() const;

  /* Methods related to task submission. */

  /// Submit a normal task.
  ///
  /// \param[in] function The remote function to execute.
  /// \param[in] args Arguments of this task.
  /// \param[in] task_options Options for this task.
  /// \param[out] return_ids Ids of the return objects.
  /// \return Status error if task submission fails, likely due to raylet failure.
  Status SubmitTask(const RayFunction &function, const std::vector<TaskArg> &args,
                    const TaskOptions &task_options, std::vector<ObjectID> *return_ids);

  /// Create an actor.
  ///
  /// \param[in] caller_id ID of the task submitter.
  /// \param[in] function The remote function that generates the actor object.
  /// \param[in] args Arguments of this task.
  /// \param[in] actor_creation_options Options for this actor creation task.
  /// \param[out] actor_handle Handle to the actor.
  /// \param[out] actor_id ID of the created actor. This can be used to submit
  /// tasks on the actor.
  /// \return Status error if actor creation fails, likely due to raylet failure.
  Status CreateActor(const RayFunction &function, const std::vector<TaskArg> &args,
                     const ActorCreationOptions &actor_creation_options,
                     ActorID *actor_id);

  /// Submit an actor task.
  ///
  /// \param[in] caller_id ID of the task submitter.
  /// \param[in] actor_handle Handle to the actor.
  /// \param[in] function The remote function to execute.
  /// \param[in] args Arguments of this task.
  /// \param[in] task_options Options for this task.
  /// \param[out] return_ids Ids of the return objects.
  /// \return Status error if the task is invalid or if the task submission
  /// failed. Tasks can be invalid for direct actor calls because not all tasks
  /// are currently supported.
  Status SubmitActorTask(const ActorID &actor_id, const RayFunction &function,
                         const std::vector<TaskArg> &args,
                         const TaskOptions &task_options,
                         std::vector<ObjectID> *return_ids);

  /// Add an actor handle from a serialized string.
  ///
  /// This should be called when an actor handle is given to us by another task
  /// or actor. This may be called even if we already have a handle to the same
  /// actor.
  ///
  /// \param[in] serialized The serialized actor handle.
  /// \return The ActorID of the deserialized handle.
  ActorID DeserializeAndRegisterActorHandle(const std::string &serialized);

  /// Serialize an actor handle.
  ///
  /// This should be called when passing an actor handle to another task or
  /// actor.
  ///
  /// \param[in] actor_id The ID of the actor handle to serialize.
  /// \param[out] The serialized handle.
  /// \return Status::Invalid if we don't have the specified handle.
  Status SerializeActorHandle(const ActorID &actor_id, std::string *output) const;

  // Add this object ID to the set of active object IDs that is sent to the raylet
  // in the heartbeat messsage.
  void AddActiveObjectID(const ObjectID &object_id);

  // Remove this object ID from the set of active object IDs that is sent to the raylet
  // in the heartbeat messsage.
  void RemoveActiveObjectID(const ObjectID &object_id);

  /// Start receiving and executing tasks.
  /// \return void.
  void StartExecutingTasks();

  /// Stop receiving and executing tasks.
  /// \return void.
  void StopExecutingTasks();

  /// Shut down the worker completely.
  /// \return void.
  void Shutdown();

 private:
  /// Give this worker a handle to an actor.
  ///
  /// This handle will remain as long as the current actor or task is
  /// executing, even if the Python handle goes out of scope. Tasks submitted
  /// through this handle are guaranteed to execute in the same order in which
  /// they are submitted.
  ///
  /// \param actor_handle The handle to the actor.
  /// \return True if the handle was added and False if we already had a handle
  /// to the same actor.
  bool AddActorHandle(std::unique_ptr<ActorHandle> actor_handle);

  /// Get a handle to an actor. This asserts that the worker actually has this
  /// handle.
  ///
  /// \param[in] actor_id The actor handle to get.
  /// \param[out] actor_handle A handle to the requested actor.
  /// \return Status::Invalid if we don't have this actor handle.
  Status GetActorHandle(const ActorID &actor_id, ActorHandle **actor_handle) const;

  /// Execute a task.
  ///
  /// \param spec[in] Task specification.
  /// \param spec[in] Resource IDs of resources assigned to this worker.
  /// \param results[out] Results for task execution.
  /// \return Status.
  Status ExecuteTask(const TaskSpecification &task_spec,
                     const ResourceMappingType &resource_ids,
                     std::vector<std::shared_ptr<RayObject>> *results);

  /// Build arguments for task executor. This would loop through all the arguments
  /// in task spec, and for each of them that's passed by reference (ObjectID),
  /// fetch its content from store and; for arguments that are passed by value,
  /// just copy their content.
  ///
  /// \param spec[in] Task specification.
  /// \param args[out] Argument data as RayObjects.
  /// \param args[out] ObjectIDs corresponding to each by reference argument. The length
  ///                  of this vector will be the same as args, and by value arguments
  ///                  will have ObjectID::Nil().
  ///                  // TODO(edoakes): this is a bit of a hack that's necessary because
  ///                  we have separate serialization paths for by-value and by-reference
  ///                  arguments in Python. This should ideally be handled better there.
  /// \return The arguments for passing to task executor.
  Status BuildArgsForExecutor(const TaskSpecification &task,
                              std::vector<std::shared_ptr<RayObject>> *args,
                              std::vector<ObjectID> *arg_reference_ids);

  void StartIOService();

  void ReportActiveObjectIDs();

  const WorkerType worker_type_;
  const Language language_;
  const std::string raylet_socket_;
  const std::string log_dir_;
  WorkerContext worker_context_;
  /// The ID of the current task being executed by the main thread. If there
  /// are multiple threads, they will have a thread-local task ID stored in the
  /// worker context.
  TaskID main_thread_task_id_;
  /// Our actor ID. If this is nil, then we execute only stateless tasks.
  ActorID actor_id_;

  // Flag indicating whether this worker has been shut down.
  bool shutdown_ = false;

  /// Event loop where the IO events are handled. e.g. async GCS operations.
  boost::asio::io_service io_service_;

  /// Keeps the io_service_ alive.
  boost::asio::io_service::work io_work_;

  /// Timer used to periodically send heartbeat containing active object IDs to the
  /// raylet.
  boost::asio::steady_timer heartbeat_timer_;

  // Thread that runs a boost::asio service to process IO events.
  std::thread io_thread_;

  // Task execution callback.
  TaskExecutionCallback task_execution_callback_;

  /// RPC server used to receive tasks to execute.
  rpc::GrpcServer worker_server_;

  // Client to the GCS shared by core worker interfaces.
  gcs::RedisGcsClient gcs_client_;

  // Client to the raylet shared by core worker interfaces.
  std::unique_ptr<RayletClient> raylet_client_;

  // Interface to submit tasks directly to other actors.
  std::unique_ptr<CoreWorkerDirectActorTaskSubmitter> direct_actor_submitter_;

  // Interface for storing and retrieving shared objects.
  CoreWorkerObjectInterface object_interface_;

  // Profiler including a background thread that pushes profiling events to the GCS.
  std::shared_ptr<worker::Profiler> profiler_;

  // Profile event for when the worker is idle. Should be reset when the worker
  // enters and exits an idle period.
  std::unique_ptr<worker::ProfileEvent> idle_profile_event_;

  /// Map from actor ID to a handle to that actor.
  std::unordered_map<ActorID, std::unique_ptr<ActorHandle>> actor_handles_;

  /// Set of object IDs that are in scope in the language worker.
  std::unordered_set<ObjectID> active_object_ids_;

  /// Indicates whether or not the active_object_ids map has changed since the
  /// last time it was sent to the raylet.
  bool active_object_ids_updated_ = false;

  /// A map from resource name to the resource IDs that are currently reserved
  /// for this worker. Each pair consists of the resource ID and the fraction
  /// of that resource allocated for this worker.
  ResourceMappingType resource_ids_;

  /// Event loop where tasks are processed.
  std::shared_ptr<boost::asio::io_service> task_execution_service_;

  /// The asio work to keep task_execution_service_ alive.
  boost::asio::io_service::work main_work_;

  /// All the task receivers supported.
  EnumUnorderedMap<TaskTransportType, std::unique_ptr<CoreWorkerTaskReceiver>>
      task_receivers_;

  friend class CoreWorkerTest;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_CORE_WORKER_H
