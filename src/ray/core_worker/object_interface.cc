#include "ray/core_worker/object_interface.h"
#include "ray/common/ray_config.h"

namespace ray {

void GetObjectIdsPerTransport(
    const std::vector<ObjectID> &object_ids,
    EnumUnorderedMap<TaskTransportType, std::vector<ObjectID>> *results) {

  for (const auto &object_id : object_ids) {
    if (object_id.IsReturnObject()) {
      auto type = static_cast<TaskTransportType>(object_id.GetTransportType());
      (*results)[type].push_back(object_id);
    } else {
      // For non-return objects, treat them the same as return objects
      // that use raylet transport.
      (*results)[TaskTransportType::RAYLET].push_back(object_id);
    }
  }
}

CoreWorkerObjectInterface::CoreWorkerObjectInterface(
    WorkerContext &worker_context, std::unique_ptr<RayletClient> &raylet_client,
    CoreWorkerStoreProviderLayer &store_provider_layer,
    CoreWorkerTaskSubmitterLayer &task_submitter_layer)
    : worker_context_(worker_context),
      raylet_client_(raylet_client),
      store_provider_layer_(store_provider_layer),
      task_submitter_layer_(task_submitter_layer) {
}

Status CoreWorkerObjectInterface::Put(const RayObject &object, ObjectID *object_id) {
  ObjectID put_id = ObjectID::ForPut(worker_context_.GetCurrentTaskID(),
                                     worker_context_.GetNextPutIndex(),
                                     /*transport_type=*/0);
  *object_id = put_id;
  return Put(object, put_id);
}

Status CoreWorkerObjectInterface::Put(const RayObject &object,
                                      const ObjectID &object_id) {
  // `ray.put` always writes objects to plasma.                              
  return store_provider_layer_.Put(StoreProviderType::PLASMA, object, object_id);
}

Status CoreWorkerObjectInterface::Get(const std::vector<ObjectID> &ids,
                                      int64_t timeout_ms,
                                      std::vector<std::shared_ptr<RayObject>> *results) {
  // TODO(zhijunfu): should determine transport based on object id, and then
  // find the store provider that the transport is using.
  
  // There can be a few cases here:
  // - for task return objects, find the store provider type for an object from
  //   its transport, and then try to get from the corresponding store provider;
  // - for other objects, try to get from plasma store.
  EnumUnorderedMap<TaskTransportType, std::vector<ObjectID>> object_ids_per_transport;
  GetObjectIdsPerTransport(ids, &object_ids_per_transport);

  for (const auto &entry : object_ids_per_transport) {
    if (entry.first == TaskTransportType::RAYLET) {
      RAY_RETURN_NOT_OK(store_provider_layer_.Get(StoreProviderType::PLASMA,
          entry.second, timeout_ms, results));
    } else {
      auto store_provider_type =
          task_submitter_layer_.GetStoreProviderTypeForReturnObject(entry.first);
      RAY_RETURN_NOT_OK(store_provider_layer_.Get(store_provider_type,
          entry.second, timeout_ms, results));
    }
  }

  return Status::OK();
}

Status CoreWorkerObjectInterface::Wait(const std::vector<ObjectID> &object_ids,
                                       int num_objects, int64_t timeout_ms,
                                       std::vector<bool> *results) {
  // TODO: if this is plasma, call the raylet client's wait.
  // otherwise, use a loop to invoke 
  return store_provider_layer_.Wait(StoreProviderType::PLASMA,
      object_ids, num_objects, timeout_ms, results);
}

Status CoreWorkerObjectInterface::Delete(const std::vector<ObjectID> &object_ids,
                                         bool local_only, bool delete_creating_tasks) {
  return store_provider_layer_.Delete(StoreProviderType::PLASMA, object_ids, local_only,
                                      delete_creating_tasks);
}

}  // namespace ray
