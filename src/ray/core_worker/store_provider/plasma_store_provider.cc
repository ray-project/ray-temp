#include "ray/core_worker/store_provider/plasma_store_provider.h"
#include "ray/common/ray_config.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/object_interface.h"
#include "ray/protobuf/gcs.pb.h"

namespace ray {

CoreWorkerPlasmaStoreProvider::CoreWorkerPlasmaStoreProvider(
    const WorkerContext &worker_context, const std::string &store_socket,
    std::unique_ptr<RayletClient> &raylet_client)
    : worker_context_(worker_context),
      local_store_provider_(store_socket),
      raylet_client_(raylet_client) {}

Status CoreWorkerPlasmaStoreProvider::Put(const RayObject &object,
                                          const ObjectID &object_id) {
  return local_store_provider_.Put(object, object_id);
}

Status CoreWorkerPlasmaStoreProvider::Get(
    const std::vector<ObjectID> &ids, int64_t timeout_ms,
    std::vector<std::shared_ptr<RayObject>> *results) {
  (*results).resize(ids.size(), nullptr);
  std::unordered_map<ObjectID, std::shared_ptr<RayObject>> objects;

  const TaskID &task_id = worker_context_.GetCurrentTaskID();
  bool was_blocked = false;

  std::unordered_set<ObjectID> unready;
  for (size_t i = 0; i < ids.size(); i++) {
    unready.insert(ids[i]);
  }

  int num_attempts = 0;
  bool should_break = false;
  int64_t remaining_timeout = timeout_ms;
  // Repeat until we get all objects.
  while (!unready.empty() && !should_break) {
    std::vector<ObjectID> unready_ids;
    for (const auto &entry : unready) {
      unready_ids.push_back(entry);
    }

    // For the initial fetch, we only fetch the objects, do not reconstruct them.
    bool fetch_only = num_attempts == 0;
    if (!fetch_only) {
      // If fetch_only is false, this worker will be blocked.
      was_blocked = true;
    }

    // TODO(zhijunfu): can call `fetchOrReconstruct` in batches as an optimization.
    RAY_CHECK_OK(raylet_client_->FetchOrReconstruct(unready_ids, fetch_only, task_id));

    // Get the objects from the object store, and parse the result.
    int64_t get_timeout;
    if (remaining_timeout >= 0) {
      get_timeout =
          std::min(remaining_timeout, RayConfig::instance().get_timeout_milliseconds());
      remaining_timeout -= get_timeout;
      should_break = remaining_timeout <= 0;
    } else {
      get_timeout = RayConfig::instance().get_timeout_milliseconds();
    }

    std::vector<std::shared_ptr<RayObject>> result_objects;
    RAY_RETURN_NOT_OK(
        local_store_provider_.Get(unready_ids, get_timeout, &result_objects));

    for (size_t i = 0; i < result_objects.size(); i++) {
      if (result_objects[i] != nullptr) {
        const auto &object_id = unready_ids[i];
        objects.emplace(object_id, result_objects[i]);
        unready.erase(object_id);
        if (result_objects[i]->IsException()) {
          should_break = true;
        }
      }
    }

    num_attempts += 1;
    CoreWorkerStoreProvider::WarnIfAttemptedTooManyTimes(num_attempts, unready);
  }

  if (was_blocked) {
    RAY_CHECK_OK(raylet_client_->NotifyUnblocked(task_id));
  }

  for (size_t i = 0; i < ids.size(); i++) {
    auto iter = objects.find(ids[i]);
    if (iter != objects.end()) {
      (*results)[i] = iter->second;
    }
  }

  return Status::OK();
}

Status CoreWorkerPlasmaStoreProvider::Wait(const std::vector<ObjectID> &object_ids,
                                           int num_objects, int64_t timeout_ms,
                                           std::vector<bool> *results) {
  const TaskID &task_id = worker_context_.GetCurrentTaskID();
  WaitResultPair result_pair;
  auto status = raylet_client_->Wait(object_ids, num_objects, timeout_ms, false, task_id,
                                     &result_pair);
  std::unordered_set<ObjectID> ready_ids;
  for (const auto &entry : result_pair.first) {
    ready_ids.insert(entry);
  }

  // TODO(zhijunfu): change RayletClient::Wait() to return a bit set, so that we don't
  // need to do this translation.
  (*results).resize(object_ids.size());
  for (size_t i = 0; i < object_ids.size(); i++) {
    (*results)[i] = ready_ids.count(object_ids[i]) > 0;
  }

  return status;
}

Status CoreWorkerPlasmaStoreProvider::Delete(const std::vector<ObjectID> &object_ids,
                                             bool local_only,
                                             bool delete_creating_tasks) {
  return raylet_client_->FreeObjects(object_ids, local_only, delete_creating_tasks);
}

}  // namespace ray
