#include "ray/core_worker/transport/direct_task_transport.h"
#include "ray/core_worker/transport/direct_actor_transport.h"

namespace ray {

void DoInlineObjectValue(const ObjectID &obj_id, std::shared_ptr<RayObject> value,
                         TaskSpecification &task) {
  auto &msg = task.GetMutableMessage();
  bool found = false;
  for (size_t i = 0; i < task.NumArgs(); i++) {
    auto count = task.ArgIdCount(i);
    if (count > 0) {
      const auto &id = task.ArgId(i, 0);
      if (id == obj_id) {
        auto *mutable_arg = msg.mutable_args(i);
        mutable_arg->clear_object_ids();
        if (value->IsInPlasmaError()) {
          // Promote the object id to plasma.
          mutable_arg->add_object_ids(
              obj_id.WithTransportType(TaskTransportType::RAYLET).Binary());
        } else {
          // Inline the object value.
          if (value->HasData()) {
            const auto &data = value->GetData();
            mutable_arg->set_data(data->Data(), data->Size());
          }
          if (value->HasMetadata()) {
            const auto &metadata = value->GetMetadata();
            mutable_arg->set_metadata(metadata->Data(), metadata->Size());
          }
        }
        found = true;
      }
    }
  }
  RAY_CHECK(found) << "obj id " << obj_id << " not found";
}

void LocalDependencyResolver::ResolveDependencies(const TaskSpecification &task,
                                                  std::function<void()> on_complete) {
  absl::flat_hash_set<ObjectID> local_dependencies;
  for (size_t i = 0; i < task.NumArgs(); i++) {
    auto count = task.ArgIdCount(i);
    if (count > 0) {
      RAY_CHECK(count <= 1) << "multi args not implemented";
      const auto &id = task.ArgId(i, 0);
      if (id.IsDirectCallType()) {
        local_dependencies.insert(id);
      }
    }
  }
  if (local_dependencies.empty()) {
    on_complete();
    return;
  }

  // This is deleted when the last dependency fetch callback finishes.
  std::shared_ptr<TaskState> state =
      std::shared_ptr<TaskState>(new TaskState{task, std::move(local_dependencies)});
  num_pending_ += 1;

  for (const auto &obj_id : state->local_dependencies) {
    in_memory_store_->GetAsync(
        obj_id, [this, state, obj_id, on_complete](std::shared_ptr<RayObject> obj) {
          RAY_CHECK(obj != nullptr);
          bool complete = false;
          {
            absl::MutexLock lock(&mu_);
            state->local_dependencies.erase(obj_id);
            DoInlineObjectValue(obj_id, obj, state->task);
            if (state->local_dependencies.empty()) {
              complete = true;
              num_pending_ -= 1;
            }
          }
          if (complete) {
            on_complete();
          }
        });
  }
}

Status CoreWorkerDirectTaskSubmitter::SubmitTask(TaskSpecification task_spec) {
  resolver_.ResolveDependencies(task_spec, [this, task_spec]() {
    // TODO(ekl) should have a queue per distinct resource type required
    absl::MutexLock lock(&mu_);
    RequestNewWorkerIfNeeded(task_spec);
    queued_tasks_.push_back(task_spec);
    // The task is now queued and will be picked up by the next leased or newly
    // idle worker. We are guaranteed a worker will show up since we called
    // RequestNewWorkerIfNeeded() earlier while holding mu_.
  });
  return Status::OK();
}

void CoreWorkerDirectTaskSubmitter::HandleWorkerLeaseGranted(const WorkerAddress &addr, std::shared_ptr<WorkerLeaseInterface> &lease_client) {
  // Setup client state for this worker.
  {
    absl::MutexLock lock(&mu_);
    worker_request_pending_ = false;

    auto it = client_cache_.find(addr);
    if (it == client_cache_.end()) {
      client_cache_[addr] =
      {std::shared_ptr<rpc::CoreWorkerClientInterface>(client_factory_(addr)),
        lease_client};
      RAY_LOG(INFO) << "Connected to " << addr.first << ":" << addr.second;
    }
  }

  // Try to assign it work.
  OnWorkerIdle(addr, /*error=*/false);
}

void CoreWorkerDirectTaskSubmitter::OnWorkerIdle(const WorkerAddress &addr,
                                                 bool was_error) {
  absl::MutexLock lock(&mu_);
  if (queued_tasks_.empty() || was_error) {
    auto &lease_client = client_cache_[addr].second;
    RAY_CHECK_OK(lease_client->ReturnWorker(addr.second));
  } else {
    auto &client = *client_cache_[addr].first;
    PushNormalTask(addr, client, queued_tasks_.front());
    queued_tasks_.pop_front();
  }
  // We have a queue of tasks, try to request more workers.
  if (!queued_tasks_.empty()) {
    RequestNewWorkerIfNeeded(queued_tasks_.front());
  }
}

void CoreWorkerDirectTaskSubmitter::RequestNewWorkerIfNeeded(
    const TaskSpecification &resource_spec, const rpc::Address *address) {
  RAY_CHECK(resource_spec.GetMessage().task_id().size() > 0);
  if (worker_request_pending_) {
    return;
  }

  std::shared_ptr<WorkerLeaseInterface> lease_client;
  if (address && address->raylet_id() != "") {
    // Connect to raylet.
    ClientID raylet_id = ClientID::FromBinary(address->raylet_id());
    auto it = remote_lease_clients_.find(raylet_id);
    if (it == remote_lease_clients_.end()) {
      RAY_LOG(DEBUG) << "Connecting to raylet " << raylet_id;
      it =
          remote_lease_clients_.emplace(raylet_id, lease_client_factory_(*address)).first;
    }
    RAY_LOG(DEBUG) << "Sending " << resource_spec.TaskId() << " to raylet " << raylet_id;
    lease_client = it->second;
  } else {
    lease_client = local_lease_client_;
  }

  // NOTE(swang): We must copy the resource spec here because the resource spec
  // may get swapped out by the time the callback fires.
  TaskSpecification resource_spec_copy(resource_spec.GetMessage());
  RAY_CHECK_OK(lease_client->RequestWorkerLease(
      resource_spec_copy, [this, resource_spec_copy, lease_client](const Status &status,
                                                     const rpc::WorkerLeaseReply &reply) mutable {
        if (status.ok()) {
          if (reply.raylet_id() == "") {
            RAY_LOG(DEBUG) << "Lease granted " << resource_spec_copy.TaskId();
            HandleWorkerLeaseGranted({reply.address(), reply.port()}, lease_client);
          } else {
            absl::MutexLock lock(&mu_);
            worker_request_pending_ = false;
            rpc::Address address;
            address.set_ip_address(reply.address());
            address.set_port(reply.port());
            address.set_raylet_id(reply.raylet_id());
            RequestNewWorkerIfNeeded(resource_spec_copy, &address);
          }
        } else {
          RAY_LOG(DEBUG) << "Retrying lease request " << resource_spec_copy.TaskId();
          // Retry the worker lease request. TODO(swang): Fail after some
          // number of attempts.
          absl::MutexLock lock(&mu_);
          worker_request_pending_ = false;
          RequestNewWorkerIfNeeded(resource_spec_copy);
        }
      }));
  worker_request_pending_ = true;
}

void CoreWorkerDirectTaskSubmitter::PushNormalTask(const WorkerAddress &addr,
                                                   rpc::CoreWorkerClientInterface &client,
                                                   TaskSpecification &task_spec) {
  auto task_id = task_spec.TaskId();
  auto num_returns = task_spec.NumReturns();
  auto request = std::unique_ptr<rpc::PushTaskRequest>(new rpc::PushTaskRequest);
  request->mutable_task_spec()->Swap(&task_spec.GetMutableMessage());
  auto status = client.PushNormalTask(
      std::move(request),
      [this, task_id, num_returns, addr](Status status, const rpc::PushTaskReply &reply) {
        OnWorkerIdle(addr, /*error=*/!status.ok());
        if (!status.ok()) {
          TreatTaskAsFailed(task_id, num_returns, rpc::ErrorType::WORKER_DIED,
                            in_memory_store_);
          return;
        }
        WriteObjectsToMemoryStore(reply, in_memory_store_);
      });
  if (!status.ok()) {
    TreatTaskAsFailed(task_id, num_returns, rpc::ErrorType::WORKER_DIED,
                      in_memory_store_);
  }
}
};  // namespace ray
