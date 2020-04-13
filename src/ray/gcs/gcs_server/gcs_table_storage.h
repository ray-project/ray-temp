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

#ifndef RAY_GCS_GCS_TABLE_STORAGE_H_
#define RAY_GCS_GCS_TABLE_STORAGE_H_

#include <utility>

#include "ray/gcs/store_client/redis_store_client.h"
#include "ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

using rpc::ActorCheckpointData;
using rpc::ActorCheckpointIdData;
using rpc::ActorTableData;
using rpc::ErrorTableData;
using rpc::GcsNodeInfo;
using rpc::HeartbeatBatchTableData;
using rpc::HeartbeatTableData;
using rpc::JobTableData;
using rpc::ObjectTableData;
using rpc::ObjectTableDataList;
using rpc::ProfileTableData;
using rpc::ResourceMap;
using rpc::ResourceTableData;
using rpc::TaskLeaseData;
using rpc::TaskReconstructionData;
using rpc::TaskTableData;
using rpc::WorkerFailureData;

/// \class GcsTable
///
/// GcsTable is the storage interface for all GCS tables whose data do not belong to
/// specific jobs. This class is not meant to be used directly. All gcs table classes
/// without job id should derive from this class and override the table_name_ member with
/// a unique value for that table.
template <typename Key, typename Data>
class GcsTable {
 public:
  explicit GcsTable(std::shared_ptr<StoreClient<Key, Data, JobID>> store_client)
      : store_client_(store_client) {}

  virtual ~GcsTable() = default;

  virtual Status Put(const Key &key, const Data &value, const StatusCallback &callback);

  Status Get(const Key &key, const OptionalItemCallback<Data> &callback);

  Status GetAll(const SegmentedCallback<std::pair<Key, Data>> &callback);

  Status Delete(const Key &key, const StatusCallback &callback);

  Status BatchDelete(const std::vector<Key> &keys, const StatusCallback &callback);

 protected:
  std::string table_name_;
  std::shared_ptr<StoreClient<Key, Data, JobID>> store_client_;
};

/// \class GcsTableWithJobId
///
/// GcsTableWithJobId is the storage interface for all GCS tables whose data belongs to
/// specific jobs. This class is not meant to be used directly. All gcs table classes with
/// job id should derive from this class and override the table_name_ member with a unique
/// value for that table.
template <typename Key, typename Data>
class GcsTableWithJobId : public GcsTable<Key, Data> {
 public:
  explicit GcsTableWithJobId(std::shared_ptr<StoreClient<Key, Data, JobID>> store_client)
      : GcsTable<Key, Data>(store_client) {}

  Status Put(const Key &key, const Data &value, const StatusCallback &callback) override;

  Status GetByJobId(const JobID &job_id,
                    const SegmentedCallback<std::pair<Key, Data>> &callback);

  Status DeleteByJobId(const JobID &job_id, const StatusCallback &callback);

 protected:
  virtual JobID GetJobIdFromKey(const Key &key) = 0;
};

class GcsJobTable : public GcsTable<JobID, JobTableData> {
 public:
  explicit GcsJobTable(
      std::shared_ptr<StoreClient<JobID, JobTableData, JobID>> store_client)
      : GcsTable(std::move(store_client)) {
    table_name_ = TablePrefix_Name(TablePrefix::JOB);
  }
};

class GcsActorTable : public GcsTableWithJobId<ActorID, ActorTableData> {
 public:
  explicit GcsActorTable(
      std::shared_ptr<StoreClient<ActorID, ActorTableData, JobID>> store_client)
      : GcsTableWithJobId(std::move(store_client)) {
    table_name_ = TablePrefix_Name(TablePrefix::ACTOR);
  }

 private:
  JobID GetJobIdFromKey(const ActorID &key) { return key.JobId(); }
};

class GcsActorCheckpointTable : public GcsTable<ActorCheckpointID, ActorCheckpointData> {
 public:
  explicit GcsActorCheckpointTable(
      std::shared_ptr<StoreClient<ActorCheckpointID, ActorCheckpointData, JobID>>
          store_client)
      : GcsTable(std::move(store_client)) {
    table_name_ = TablePrefix_Name(TablePrefix::ACTOR_CHECKPOINT);
  }
};

class GcsActorCheckpointIdTable
    : public GcsTableWithJobId<ActorID, ActorCheckpointIdData> {
 public:
  explicit GcsActorCheckpointIdTable(
      std::shared_ptr<StoreClient<ActorID, ActorCheckpointIdData, JobID>> store_client)
      : GcsTableWithJobId(std::move(store_client)) {
    table_name_ = TablePrefix_Name(TablePrefix::ACTOR_CHECKPOINT_ID);
  }

 private:
  JobID GetJobIdFromKey(const ActorID &key) { return key.JobId(); }
};

class GcsTaskTable : public GcsTableWithJobId<TaskID, TaskTableData> {
 public:
  explicit GcsTaskTable(
      std::shared_ptr<StoreClient<TaskID, TaskTableData, JobID>> store_client)
      : GcsTableWithJobId(std::move(store_client)) {
    table_name_ = TablePrefix_Name(TablePrefix::TASK);
  }

 private:
  JobID GetJobIdFromKey(const TaskID &key) { return key.ActorId().JobId(); }
};

class GcsTaskLeaseTable : public GcsTableWithJobId<TaskID, TaskLeaseData> {
 public:
  explicit GcsTaskLeaseTable(
      std::shared_ptr<StoreClient<TaskID, TaskLeaseData, JobID>> store_client)
      : GcsTableWithJobId(std::move(store_client)) {
    table_name_ = TablePrefix_Name(TablePrefix::TASK_LEASE);
  }

 private:
  JobID GetJobIdFromKey(const TaskID &key) { return key.ActorId().JobId(); }
};

class GcsTaskReconstructionTable
    : public GcsTableWithJobId<TaskID, TaskReconstructionData> {
 public:
  explicit GcsTaskReconstructionTable(
      std::shared_ptr<StoreClient<TaskID, TaskReconstructionData, JobID>> store_client)
      : GcsTableWithJobId(std::move(store_client)) {
    table_name_ = TablePrefix_Name(TablePrefix::TASK_RECONSTRUCTION);
  }

 private:
  JobID GetJobIdFromKey(const TaskID &key) { return key.ActorId().JobId(); }
};

class GcsObjectTable : public GcsTableWithJobId<ObjectID, ObjectTableDataList> {
 public:
  explicit GcsObjectTable(
      std::shared_ptr<StoreClient<ObjectID, ObjectTableDataList, JobID>> store_client)
      : GcsTableWithJobId(std::move(store_client)) {
    table_name_ = TablePrefix_Name(TablePrefix::OBJECT);
  }

 private:
  JobID GetJobIdFromKey(const ObjectID &key) { return key.TaskId().JobId(); }
};

class GcsNodeTable : public GcsTable<ClientID, GcsNodeInfo> {
 public:
  explicit GcsNodeTable(
      std::shared_ptr<StoreClient<ClientID, GcsNodeInfo, JobID>> store_client)
      : GcsTable(std::move(store_client)) {
    table_name_ = TablePrefix_Name(TablePrefix::CLIENT);
  }
};

class GcsNodeResourceTable : public GcsTable<ClientID, ResourceMap> {
 public:
  explicit GcsNodeResourceTable(
      std::shared_ptr<StoreClient<ClientID, ResourceMap, JobID>> store_client)
      : GcsTable(std::move(store_client)) {
    table_name_ = TablePrefix_Name(TablePrefix::NODE_RESOURCE);
  }
};

class GcsHeartbeatTable : public GcsTable<ClientID, HeartbeatTableData> {
 public:
  explicit GcsHeartbeatTable(
      std::shared_ptr<StoreClient<ClientID, HeartbeatTableData, JobID>> store_client)
      : GcsTable(std::move(store_client)) {
    table_name_ = TablePrefix_Name(TablePrefix::HEARTBEAT);
  }
};

class GcsHeartbeatBatchTable : public GcsTable<ClientID, HeartbeatBatchTableData> {
 public:
  explicit GcsHeartbeatBatchTable(
      std::shared_ptr<StoreClient<ClientID, HeartbeatBatchTableData, JobID>> store_client)
      : GcsTable(std::move(store_client)) {
    table_name_ = TablePrefix_Name(TablePrefix::HEARTBEAT_BATCH);
  }
};

class GcsErrorInfoTable : public GcsTable<JobID, ErrorTableData> {
 public:
  explicit GcsErrorInfoTable(
      std::shared_ptr<StoreClient<JobID, ErrorTableData, JobID>> store_client)
      : GcsTable(std::move(store_client)) {
    table_name_ = TablePrefix_Name(TablePrefix::ERROR_INFO);
  }
};

class GcsProfileTable : public GcsTable<UniqueID, ProfileTableData> {
 public:
  explicit GcsProfileTable(
      std::shared_ptr<StoreClient<UniqueID, ProfileTableData, JobID>> store_client)
      : GcsTable(std::move(store_client)) {
    table_name_ = TablePrefix_Name(TablePrefix::PROFILE);
  }
};

class GcsWorkerFailureTable : public GcsTable<WorkerID, WorkerFailureData> {
 public:
  explicit GcsWorkerFailureTable(
      std::shared_ptr<StoreClient<WorkerID, WorkerFailureData, JobID>> store_client)
      : GcsTable(std::move(store_client)) {
    table_name_ = TablePrefix_Name(TablePrefix::WORKER_FAILURE);
  }
};

/// \class GcsTableStorage
///
/// This class is not meant to be used directly. All gcs table storage classes should
/// derive from this class and override class member variables.
class GcsTableStorage {
 public:
  GcsJobTable &JobTable() {
    RAY_CHECK(job_table_ != nullptr);
    return *job_table_;
  }

  GcsActorTable &ActorTable() {
    RAY_CHECK(actor_table_ != nullptr);
    return *actor_table_;
  }

  GcsActorCheckpointTable &ActorCheckpointTable() {
    RAY_CHECK(actor_checkpoint_table_ != nullptr);
    return *actor_checkpoint_table_;
  }

  GcsActorCheckpointIdTable &ActorCheckpointIdTable() {
    RAY_CHECK(actor_checkpoint_id_table_ != nullptr);
    return *actor_checkpoint_id_table_;
  }

  GcsTaskTable &TaskTable() {
    RAY_CHECK(task_table_ != nullptr);
    return *task_table_;
  }

  GcsTaskLeaseTable &TaskLeaseTable() {
    RAY_CHECK(task_lease_table_ != nullptr);
    return *task_lease_table_;
  }

  GcsTaskReconstructionTable &TaskReconstructionTable() {
    RAY_CHECK(task_reconstruction_table_ != nullptr);
    return *task_reconstruction_table_;
  }

  GcsObjectTable &ObjectTable() {
    RAY_CHECK(object_table_ != nullptr);
    return *object_table_;
  }

  GcsNodeTable &NodeTable() {
    RAY_CHECK(node_table_ != nullptr);
    return *node_table_;
  }

  GcsNodeResourceTable &NodeResourceTable() {
    RAY_CHECK(node_resource_table_ != nullptr);
    return *node_resource_table_;
  }

  GcsHeartbeatTable &HeartbeatTable() {
    RAY_CHECK(heartbeat_table_ != nullptr);
    return *heartbeat_table_;
  }

  GcsHeartbeatBatchTable &HeartbeatBatchTable() {
    RAY_CHECK(heartbeat_batch_table_ != nullptr);
    return *heartbeat_batch_table_;
  }

  GcsErrorInfoTable &ErrorInfoTable() {
    RAY_CHECK(error_info_table_ != nullptr);
    return *error_info_table_;
  }

  GcsProfileTable &ProfileTable() {
    RAY_CHECK(profile_table_ != nullptr);
    return *profile_table_;
  }

  GcsWorkerFailureTable &WorkerFailureTable() {
    RAY_CHECK(worker_failure_table_ != nullptr);
    return *worker_failure_table_;
  }

 protected:
  std::unique_ptr<GcsJobTable> job_table_;
  std::unique_ptr<GcsActorTable> actor_table_;
  std::unique_ptr<GcsActorCheckpointTable> actor_checkpoint_table_;
  std::unique_ptr<GcsActorCheckpointIdTable> actor_checkpoint_id_table_;
  std::unique_ptr<GcsTaskTable> task_table_;
  std::unique_ptr<GcsTaskLeaseTable> task_lease_table_;
  std::unique_ptr<GcsTaskReconstructionTable> task_reconstruction_table_;
  std::unique_ptr<GcsObjectTable> object_table_;
  std::unique_ptr<GcsNodeTable> node_table_;
  std::unique_ptr<GcsNodeResourceTable> node_resource_table_;
  std::unique_ptr<GcsHeartbeatTable> heartbeat_table_;
  std::unique_ptr<GcsHeartbeatBatchTable> heartbeat_batch_table_;
  std::unique_ptr<GcsErrorInfoTable> error_info_table_;
  std::unique_ptr<GcsProfileTable> profile_table_;
  std::unique_ptr<GcsWorkerFailureTable> worker_failure_table_;
};

/// \class RedisGcsTableStorage
/// RedisGcsTableStorage is an implementation of `GcsTableStorage`
/// that uses redis as storage.
class RedisGcsTableStorage : public GcsTableStorage {
 public:
  explicit RedisGcsTableStorage(std::shared_ptr<RedisClient> redis_client) {
    job_table_.reset(new GcsJobTable(
        std::make_shared<RedisStoreClient<JobID, JobTableData, JobID>>(redis_client)));
    actor_table_.reset(new GcsActorTable(
        std::make_shared<RedisStoreClient<ActorID, ActorTableData, JobID>>(
            redis_client)));
    actor_checkpoint_table_.reset(new GcsActorCheckpointTable(
        std::make_shared<RedisStoreClient<ActorCheckpointID, ActorCheckpointData, JobID>>(
            redis_client)));
    actor_checkpoint_id_table_.reset(new GcsActorCheckpointIdTable(
        std::make_shared<RedisStoreClient<ActorID, ActorCheckpointIdData, JobID>>(
            redis_client)));
    task_table_.reset(new GcsTaskTable(
        std::make_shared<RedisStoreClient<TaskID, TaskTableData, JobID>>(redis_client)));
    task_lease_table_.reset(new GcsTaskLeaseTable(
        std::make_shared<RedisStoreClient<TaskID, TaskLeaseData, JobID>>(redis_client)));
    task_reconstruction_table_.reset(new GcsTaskReconstructionTable(
        std::make_shared<RedisStoreClient<TaskID, TaskReconstructionData, JobID>>(
            redis_client)));
    object_table_.reset(new GcsObjectTable(
        std::make_shared<RedisStoreClient<ObjectID, ObjectTableDataList, JobID>>(
            redis_client)));
    node_table_.reset(new GcsNodeTable(
        std::make_shared<RedisStoreClient<ClientID, GcsNodeInfo, JobID>>(redis_client)));
    node_resource_table_.reset(new GcsNodeResourceTable(
        std::make_shared<RedisStoreClient<ClientID, ResourceMap, JobID>>(redis_client)));
    heartbeat_table_.reset(new GcsHeartbeatTable(
        std::make_shared<RedisStoreClient<ClientID, HeartbeatTableData, JobID>>(
            redis_client)));
    heartbeat_batch_table_.reset(new GcsHeartbeatBatchTable(
        std::make_shared<RedisStoreClient<ClientID, HeartbeatBatchTableData, JobID>>(
            redis_client)));
    error_info_table_.reset(new GcsErrorInfoTable(
        std::make_shared<RedisStoreClient<JobID, ErrorTableData, JobID>>(redis_client)));
    profile_table_.reset(new GcsProfileTable(
        std::make_shared<RedisStoreClient<UniqueID, ProfileTableData, JobID>>(
            redis_client)));
    worker_failure_table_.reset(new GcsWorkerFailureTable(
        std::make_shared<RedisStoreClient<WorkerID, WorkerFailureData, JobID>>(
            redis_client)));
  }
};

}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_GCS_TABLE_STORAGE_H_
