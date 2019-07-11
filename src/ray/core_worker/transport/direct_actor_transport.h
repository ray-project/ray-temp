#ifndef RAY_CORE_WORKER_DIRECT_ACTOR_TRANSPORT_H
#define RAY_CORE_WORKER_DIRECT_ACTOR_TRANSPORT_H

#include <list>

#include "ray/core_worker/transport/transport.h"
#include "ray/raylet/raylet_client.h"
#include  "ray/gcs/gcs_client.h"
#include "ray/rpc/worker/direct_actor_client.h"
#include "ray/rpc/worker/direct_actor_server.h"
#include "ray/core_worker/object_interface.h"

namespace ray {

/// In raylet task submitter and receiver, a task is submitted to raylet, and possibly
/// gets forwarded to another raylet on which node the task should be executed, and
/// then a worker on that node gets this task and starts executing it.

struct PendingTaskRequest {
  TaskID task_id;
  int num_returns;
  std::unique_ptr<rpc::PushTaskRequest> request;
};


class CoreWorkerDirectActorTaskSubmitter : public CoreWorkerTaskSubmitter {
 public:
  CoreWorkerDirectActorTaskSubmitter(
      boost::asio::io_service &io_service,
      gcs::GcsClient &gcs_client,
      CoreWorkerObjectInterface &object_interface);

  /// Submit a task for execution to raylet.
  ///
  /// \param[in] task The task spec to submit.
  /// \return Status.
  virtual Status SubmitTask(const TaskSpec &task) override;

 private:
  /// Subscribe to actor table.
  Status SubscribeActorTable();

  Status PushTask(rpc::DirectActorClient &client, const rpc::PushTaskRequest &request,
                  const TaskID &task_id, int num_returns);

  void TreatTaskAsFailed(const TaskID &task_id, int num_returns, const rpc::ErrorType &error_type);

  /// The IO event loop.
  boost::asio::io_service &io_service_;

  /// Gcs client.
  gcs::GcsClient &gcs_client_;

  /// The `ClientCallManager` object that is shared by all `DirectActorClient`s.
  rpc::ClientCallManager client_call_manager_;

  /// Mutex to proect `rpc_clients_` below.
  std::mutex rpc_clients_mutex_;

  /// Map from actor ids to direct actor call rpc clients.
  std::unordered_map<ActorID, std::unique_ptr<rpc::DirectActorClient>> rpc_clients_;

  /// Pending requests to send out on a per-actor basis.
  //std::unordered_map<ActorID, std::list<std::unique_ptr<rpc::PushTaskRequest>>> pending_requests_;
  std::unordered_map<ActorID, std::list<std::unique_ptr<PendingTaskRequest>>> pending_requests_;


  /// The store provider.
  std::unique_ptr<CoreWorkerStoreProvider> store_provider_;

  int counter_;
};

class CoreWorkerDirectActorTaskReceiver
    : public CoreWorkerTaskReceiver,
      public rpc::DirectActorHandler {
 public:
  CoreWorkerDirectActorTaskReceiver(
    CoreWorkerObjectInterface &object_interface,
    boost::asio::io_service &io_service,
    rpc::GrpcServer &server, const TaskHandler &task_handler);

  /// Handle a `PushTask` request.
  /// The implementation can handle this request asynchronously. When hanling is done, the
  /// `done_callback` should be called.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] done_callback The callback to be called when the request is done.
  void HandlePushTask(const rpc::PushTaskRequest &request,
                      rpc::PushTaskReply *reply,
                      rpc::SendReplyCallback send_reply_callback) override;


 private:
  // Object interface.
  CoreWorkerObjectInterface &object_interface_;
  /// The rpc service for `DirectActorService`.
  rpc::DirectActorGrpcService task_service_;
  /// The callback function to process a task.
  TaskHandler task_handler_;  
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_DIRECT_ACTOR_TRANSPORT_H
