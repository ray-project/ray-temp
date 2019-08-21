#include "ray/core_worker/store_provider/memory_store_provider.h"
#include <condition_variable>
#include "ray/common/ray_config.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/object_interface.h"

namespace ray {

//
// CoreWorkerMemoryStoreProvider functions
//
CoreWorkerMemoryStoreProvider::CoreWorkerMemoryStoreProvider(
    std::shared_ptr<CoreWorkerMemoryStore> store)
    : store_(store) {
  RAY_CHECK(store != nullptr);
}

Status CoreWorkerMemoryStoreProvider::Put(const RayObject &object,
                                          const ObjectID &object_id) {
  return store_->Put(object_id, object);
}

Status CoreWorkerMemoryStoreProvider::Create(const std::shared_ptr<Buffer> &metadata,
                                             const size_t data_size,
                                             const ObjectID &object_id,
                                             std::shared_ptr<Buffer> *data) {
  return Status::NotImplemented(
      "Create/Seal interface not implemented for in-memory store.");
}

Status CoreWorkerMemoryStoreProvider::Seal(const ObjectID &object_id) {
  return Status::NotImplemented(
      "Create/Seal interface not implemented for in-memory store.");
}

Status CoreWorkerMemoryStoreProvider::Get(
    const std::vector<ObjectID> &object_ids, int64_t timeout_ms, const TaskID &task_id,
    std::vector<std::shared_ptr<RayObject>> *results) {
  return store_->Get(object_ids, object_ids.size(), timeout_ms, true, results);
}

Status CoreWorkerMemoryStoreProvider::Wait(const std::vector<ObjectID> &object_ids,
                                           int num_objects, int64_t timeout_ms,
                                           const TaskID &task_id,
                                           std::vector<bool> *results) {
  (*results).resize(object_ids.size(), false);

  std::vector<std::shared_ptr<RayObject>> result_objects;
  auto status = store_->Get(object_ids, num_objects, timeout_ms, false, &result_objects);
  if (status.ok()) {
    RAY_CHECK(result_objects.size() == object_ids.size());
    for (size_t i = 0; i < object_ids.size(); i++) {
      (*results)[i] = (result_objects[i] != nullptr);
    }
  }

  return status;
}

Status CoreWorkerMemoryStoreProvider::Delete(const std::vector<ObjectID> &object_ids,
                                             bool local_only,
                                             bool delete_creating_tasks) {
  store_->Delete(object_ids);
  return Status::OK();
}

}  // namespace ray
