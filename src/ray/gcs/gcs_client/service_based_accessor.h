#ifndef RAY_GCS_SERVICE_BASED_ACCESSOR_H
#define RAY_GCS_SERVICE_BASED_ACCESSOR_H

#include "src/ray/gcs/accessor.h"
#include "src/ray/gcs/subscription_executor.h"

namespace ray {
namespace gcs {

class ServiceBasedGcsClient;

/// \class ServiceBasedJobInfoAccessor
/// ServiceBasedJobInfoAccessor is an implementation of `JobInfoAccessor`
/// that uses GCS Service as the backend storage.
class ServiceBasedJobInfoAccessor : public JobInfoAccessor {
 public:
  explicit ServiceBasedJobInfoAccessor(ServiceBasedGcsClient *client_impl);

  virtual ~ServiceBasedJobInfoAccessor() = default;

  Status AsyncAdd(const std::shared_ptr<JobTableData> &data_ptr,
                  const StatusCallback &callback) override;

  Status AsyncMarkFinished(const JobID &job_id, const StatusCallback &callback) override;

  Status AsyncSubscribeToFinishedJobs(
      const SubscribeCallback<JobID, JobTableData> &subscribe,
      const StatusCallback &done) override;

 private:
  ServiceBasedGcsClient *client_impl_{nullptr};

  typedef SubscriptionExecutor<JobID, JobTableData, JobTable> JobSubscriptionExecutor;
  JobSubscriptionExecutor job_sub_executor_;
};

/// \class ServiceBasedActorInfoAccessor
/// ServiceBasedActorInfoAccessor is an implementation of `ActorInfoAccessor`
/// that uses GCS Service as the backend storage.
class ServiceBasedActorInfoAccessor : public ActorInfoAccessor {
 public:
  explicit ServiceBasedActorInfoAccessor(ServiceBasedGcsClient *client_impl);

  virtual ~ServiceBasedActorInfoAccessor() = default;

  Status AsyncGet(const ActorID &actor_id,
                  const OptionalItemCallback<rpc::ActorTableData> &callback) override;

  Status AsyncRegister(const std::shared_ptr<rpc::ActorTableData> &data_ptr,
                       const StatusCallback &callback) override;

  Status AsyncUpdate(const ActorID &actor_id,
                     const std::shared_ptr<rpc::ActorTableData> &data_ptr,
                     const StatusCallback &callback) override;

  Status AsyncSubscribeAll(
      const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
      const StatusCallback &done) override;

  Status AsyncSubscribe(const ActorID &actor_id,
                        const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
                        const StatusCallback &done) override;

  Status AsyncUnsubscribe(const ActorID &actor_id, const StatusCallback &done) override;

  Status AsyncAddCheckpoint(const std::shared_ptr<rpc::ActorCheckpointData> &data_ptr,
                            const StatusCallback &callback) override;

  Status AsyncGetCheckpoint(
      const ActorCheckpointID &checkpoint_id,
      const OptionalItemCallback<rpc::ActorCheckpointData> &callback) override;

  Status AsyncGetCheckpointID(
      const ActorID &actor_id,
      const OptionalItemCallback<rpc::ActorCheckpointIdData> &callback) override;

 private:
  ServiceBasedGcsClient *client_impl_{nullptr};

  ClientID subscribe_id_{ClientID::FromRandom()};

  typedef SubscriptionExecutor<ActorID, ActorTableData, ActorTable>
      ActorSubscriptionExecutor;
  ActorSubscriptionExecutor actor_sub_executor_;
};

/// \class ServiceBasedNodeInfoAccessor
/// ServiceBasedNodeInfoAccessor is an implementation of `NodeInfoAccessor`
/// that uses GCS Service as the backend storage.
class ServiceBasedNodeInfoAccessor : public NodeInfoAccessor {
 public:
  explicit ServiceBasedNodeInfoAccessor(ServiceBasedGcsClient *client_impl);

  virtual ~ServiceBasedNodeInfoAccessor() = default;

  Status RegisterSelf(const GcsNodeInfo &local_node_info) override;

  Status UnregisterSelf() override;

  const ClientID &GetSelfId() const override;

  const GcsNodeInfo &GetSelfInfo() const override;

  Status AsyncRegister(const rpc::GcsNodeInfo &node_info,
                       const StatusCallback &callback) override;

  Status AsyncUnregister(const ClientID &node_id,
                         const StatusCallback &callback) override;

  Status AsyncGetAll(const MultiItemCallback<GcsNodeInfo> &callback) override;

  Status AsyncSubscribeToNodeChange(
      const SubscribeCallback<ClientID, GcsNodeInfo> &subscribe,
      const StatusCallback &done) override;

  boost::optional<GcsNodeInfo> Get(const ClientID &node_id) const override;

  const std::unordered_map<ClientID, GcsNodeInfo> &GetAll() const override;

  bool IsRemoved(const ClientID &node_id) const override;

  Status AsyncGetResources(const ClientID &node_id,
                           const OptionalItemCallback<ResourceMap> &callback) override;

  Status AsyncUpdateResources(const ClientID &node_id, const ResourceMap &resources,
                              const StatusCallback &callback) override;

  Status AsyncDeleteResources(const ClientID &node_id,
                              const std::vector<std::string> &resource_names,
                              const StatusCallback &callback) override;

  Status AsyncSubscribeToResources(
      const SubscribeCallback<ClientID, ResourceChangeNotification> &subscribe,
      const StatusCallback &done) override;

  Status AsyncReportHeartbeat(const std::shared_ptr<rpc::HeartbeatTableData> &data_ptr,
                              const StatusCallback &callback) override;

  Status AsyncSubscribeHeartbeat(
      const SubscribeCallback<ClientID, rpc::HeartbeatTableData> &subscribe,
      const StatusCallback &done) override;

  Status AsyncReportBatchHeartbeat(
      const std::shared_ptr<rpc::HeartbeatBatchTableData> &data_ptr,
      const StatusCallback &callback) override;

  Status AsyncSubscribeBatchHeartbeat(
      const ItemCallback<rpc::HeartbeatBatchTableData> &subscribe,
      const StatusCallback &done) override;

 private:
  ServiceBasedGcsClient *client_impl_{nullptr};

  typedef SubscriptionExecutor<ClientID, ResourceChangeNotification, DynamicResourceTable>
      DynamicResourceSubscriptionExecutor;
  DynamicResourceSubscriptionExecutor resource_sub_executor_;

  typedef SubscriptionExecutor<ClientID, HeartbeatTableData, HeartbeatTable>
      HeartbeatSubscriptionExecutor;
  HeartbeatSubscriptionExecutor heartbeat_sub_executor_;

  typedef SubscriptionExecutor<ClientID, HeartbeatBatchTableData, HeartbeatBatchTable>
      HeartbeatBatchSubscriptionExecutor;
  HeartbeatBatchSubscriptionExecutor heartbeat_batch_sub_executor_;

  GcsNodeInfo local_node_info_;
  ClientID local_node_id_;
};

/// \class ServiceBasedTaskInfoAccessor
/// ServiceBasedTaskInfoAccessor is an implementation of `TaskInfoAccessor`
/// that uses GCS service as the backend storage.
class ServiceBasedTaskInfoAccessor : public TaskInfoAccessor {
 public:
  explicit ServiceBasedTaskInfoAccessor(ServiceBasedGcsClient *client_impl);

  virtual ~ServiceBasedTaskInfoAccessor() = default;

  Status AsyncAdd(const std::shared_ptr<rpc::TaskTableData> &data_ptr,
                  const StatusCallback &callback) override;

  Status AsyncGet(const TaskID &task_id,
                  const OptionalItemCallback<rpc::TaskTableData> &callback) override;

  Status AsyncDelete(const std::vector<TaskID> &task_ids,
                     const StatusCallback &callback) override;

  Status AsyncSubscribe(const TaskID &task_id,
                        const SubscribeCallback<TaskID, rpc::TaskTableData> &subscribe,
                        const StatusCallback &done) override;

  Status AsyncUnsubscribe(const TaskID &task_id, const StatusCallback &done) override;

  Status AsyncAddTaskLease(const std::shared_ptr<rpc::TaskLeaseData> &data_ptr,
                           const StatusCallback &callback) override;

  Status AsyncSubscribeTaskLease(
      const TaskID &task_id,
      const SubscribeCallback<TaskID, boost::optional<rpc::TaskLeaseData>> &subscribe,
      const StatusCallback &done) override;

  Status AsyncUnsubscribeTaskLease(const TaskID &task_id,
                                   const StatusCallback &done) override;

  Status AttemptTaskReconstruction(
      const std::shared_ptr<rpc::TaskReconstructionData> &data_ptr,
      const StatusCallback &callback) override;

 private:
  ServiceBasedGcsClient *client_impl_{nullptr};

  ClientID subscribe_id_{ClientID::FromRandom()};

  typedef SubscriptionExecutor<TaskID, TaskTableData, raylet::TaskTable>
      TaskSubscriptionExecutor;
  TaskSubscriptionExecutor task_sub_executor_;

  typedef SubscriptionExecutor<TaskID, boost::optional<TaskLeaseData>, TaskLeaseTable>
      TaskLeaseSubscriptionExecutor;
  TaskLeaseSubscriptionExecutor task_lease_sub_executor_;
};

}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_SERVICE_BASED_ACCESSOR_H
