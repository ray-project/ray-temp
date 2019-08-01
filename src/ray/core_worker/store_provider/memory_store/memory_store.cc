#include <condition_variable>
#include "ray/common/ray_config.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/object_interface.h"
#include "ray/core_worker/store_provider/memory_store_provider.h"

namespace ray {

/// A class that represents a `Get` or `Wait` reuquest.
class GetOrWaitRequest {
 public:
  GetOrWaitRequest(const std::vector<ObjectID> &object_ids, bool is_get);

  const std::vector<ObjectID> &ObjectIds() const;

  /// Wait until all requested objects are available, or timeout happens.
  bool Wait(int64_t timeout_ms);
  /// Set the object content for the specific object id.
  void Set(const ObjectID &object_id, std::shared_ptr<RayObject> buffer);
  /// Get the object content for the specific object id.
  std::shared_ptr<RayObject> Get(const ObjectID &object_id) const;
  /// Whether this is a `get` request.
  bool IsGetRequest() const;

 private:
  /// Wait until all requested objects are available.
  void Wait();

  /// The object IDs involved in this request. This is used in the reply.
  const std::vector<ObjectID> object_ids_;
  /// The object information for the objects in this request.
  std::unordered_map<ObjectID, std::shared_ptr<RayObject>> objects_;

  // Whether this request is a `get` request.
  const bool is_get_;
  // Whether all the requested objects are available.
  bool is_ready_;
  mutable std::mutex mutex_;
  std::condition_variable cv_;
};

GetOrWaitRequest::GetOrWaitRequest(const std::vector<ObjectID> &object_ids, bool is_get)
    : object_ids_(object_ids), is_get_(is_get) {}

const std::vector<ObjectID> &GetOrWaitRequest::ObjectIds() const { return object_ids_; }

bool GetOrWaitRequest::IsGetRequest() const { return is_get_; }

bool GetOrWaitRequest::Wait(int64_t timeout_ms) {
  if (timeout_ms < 0) {
    // Wait forever until the object is ready.
    Wait();
    return true;
  }

  // Wait until the object is ready, or the timeout expires.
  std::unique_lock<std::mutex> lock(mutex_);
  while (!is_ready_) {
    auto status = cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms));
    if (status == std::cv_status::timeout) {
      return false;
    }
  }
  return true;
}

void GetOrWaitRequest::Wait() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (!is_ready_) {
    cv_.wait(lock);
  }
}

void GetOrWaitRequest::Set(const ObjectID &object_id, std::shared_ptr<RayObject> object) {
  std::unique_lock<std::mutex> lock(mutex_);
  objects_.emplace(object_id, object);
  if (objects_.size() == object_ids_.size()) {
    is_ready_ = true;
    cv_.notify_all();
  }
}

std::shared_ptr<RayObject> GetOrWaitRequest::Get(const ObjectID &object_id) const {
  std::unique_lock<std::mutex> lock(mutex_);
  auto iter = objects_.find(object_id);
  if (iter != objects_.end()) {
    return iter->second;
  }

  return nullptr;
}

CoreWorkerMemoryStore::CoreWorkerMemoryStore() {}

Status CoreWorkerMemoryStore::Put(const RayObject &object, const ObjectID &object_id) {
  std::unique_lock<std::mutex> lock(lock_);
  auto iter = objects_.find(object_id);
  if (iter != objects_.end()) {
    return Status::KeyError("object already exists");
  }

  auto object_entry =
      std::make_shared<RayObject>(object.GetData(), object.GetMetadata(), true);

  bool should_add_entry = true;
  auto object_request_iter = object_get_requests_.find(object_id);
  if (object_request_iter != object_get_requests_.end()) {
    auto &get_requests = object_request_iter->second;
    for (auto &get_req : get_requests) {
      get_req->Set(object_id, object_entry);
      if (get_req->IsGetRequest()) {
        should_add_entry = false;
      }
    }
  }

  if (should_add_entry) {
    // If there is no existing get request, then add the `RayObject` to map.
    objects_.emplace(object_id, object_entry);
  }
  return Status::OK();
}

Status CoreWorkerMemoryStore::GetOrWait(const std::vector<ObjectID> &object_ids,
                                        int64_t timeout_ms,
                                        std::vector<std::shared_ptr<RayObject>> *results,
                                        bool is_get) {
  (*results).resize(object_ids.size(), nullptr);
  std::vector<ObjectID> remaining_ids;

  std::shared_ptr<GetOrWaitRequest> get_request;

  {
    std::unique_lock<std::mutex> lock(lock_);
    // Check for existing objects and see if this get request can be fullfilled.
    for (int i = 0; i < object_ids.size(); i++) {
      const auto &object_id = object_ids[i];
      auto iter = objects_.find(object_id);
      if (iter != objects_.end()) {
        (*results)[i] = iter->second;
        if (is_get) {
          objects_.erase(object_id);
        }
      } else {
        remaining_ids.emplace_back(object_id);
      }
    }

    // Return if all the objects are obtained.
    if (remaining_ids.empty()) {
      return Status::OK();
    }

    // Otherwise, create a GetOrWaitRequest to track remaining objects.
    get_request = std::make_shared<GetOrWaitRequest>(remaining_ids, is_get);
    for (const auto &object_id : remaining_ids) {
      object_get_requests_[object_id].push_back(get_request);
    }
  }

  // Wait for remaining objects (or timeout).
  get_request->Wait(timeout_ms);

  {
    std::unique_lock<std::mutex> lock(lock_);
    // Populate results.
    for (int i = 0; i < object_ids.size(); i++) {
      const auto &object_id = object_ids[i];
      if ((*results)[i] == nullptr) {
        (*results)[i] = get_request->Get(object_id);
      }
    }

    // Remove get rquest.
    for (const auto &object_id : get_request->ObjectIds()) {
      auto object_request_iter = object_get_requests_.find(object_id);
      if (object_request_iter != object_get_requests_.end()) {
        auto &get_requests = object_request_iter->second;
        // Erase get_req from the vector.
        auto it = std::find(get_requests.begin(), get_requests.end(), get_request);
        if (it != get_requests.end()) {
          get_requests.erase(it);
          // If the vector is empty, remove the object ID from the map.
          if (get_requests.empty()) {
            object_get_requests_.erase(object_request_iter);
          }
        }
      }
    }
  }

  return Status::OK();
}

Status CoreWorkerMemoryStore::Get(const std::vector<ObjectID> &object_ids,
                                  int64_t timeout_ms,
                                  std::vector<std::shared_ptr<RayObject>> *results) {
  return GetOrWait(object_ids, timeout_ms, results, /* is_get */ true);
}

Status CoreWorkerMemoryStore::Wait(const std::vector<ObjectID> &object_ids,
                                   int num_objects, int64_t timeout_ms,
                                   std::vector<bool> *results) {
  if (num_objects != object_ids.size()) {
    return Status::Invalid("num_objects should equal to number of items in object_ids");
  }

  (*results).resize(object_ids.size(), false);

  std::vector<std::shared_ptr<RayObject>> result_objects;
  auto status = GetOrWait(object_ids, timeout_ms, &result_objects, /* is_get */ false);
  if (status.ok()) {
    RAY_CHECK(result_objects.size() == object_ids.size());
    for (int i = 0; i < object_ids.size(); i++) {
      (*results)[i] = (result_objects[i] != nullptr);
    }
  }

  return Status::OK();
}

void CoreWorkerMemoryStore::Delete(const std::vector<ObjectID> &object_ids) {
  std::unique_lock<std::mutex> lock(lock_);
  for (const auto &object_id : object_ids) {
    objects_.erase(object_id);
  }
}

}  // namespace ray
