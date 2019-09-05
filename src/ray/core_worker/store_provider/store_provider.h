#ifndef RAY_CORE_WORKER_STORE_PROVIDER_H
#define RAY_CORE_WORKER_STORE_PROVIDER_H

#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/core_worker/common.h"

namespace ray {

/// Binary representation of a ray object.
class RayObject {
 public:
  /// Create a ray object instance.
  ///
  /// \param[in] data Data of the ray object.
  /// \param[in] metadata Metadata of the ray object.
  /// \param[in] copy_data Whether this class should hold a copy of data.
  RayObject(const std::shared_ptr<Buffer> &data, const std::shared_ptr<Buffer> &metadata,
            bool copy_data = false)
      : data_(data), metadata_(metadata), has_data_copy_(copy_data) {
    if (has_data_copy_) {
      // If this object is required to hold a copy of the data,
      // make a copy if the passed in buffers don't already have a copy.
      if (data_ && !data_->OwnsData()) {
        data_ = std::make_shared<LocalMemoryBuffer>(data_->Data(), data_->Size(), true);
      }

      if (metadata_ && !metadata_->OwnsData()) {
        metadata_ = std::make_shared<LocalMemoryBuffer>(metadata_->Data(),
                                                        metadata_->Size(), true);
      }
    }
  }

  /// Return the data of the ray object.
  const std::shared_ptr<Buffer> &GetData() const { return data_; };

  /// Return the metadata of the ray object.
  const std::shared_ptr<Buffer> &GetMetadata() const { return metadata_; };

  uint64_t GetSize() const {
    uint64_t size = 0;
    size += (data_ != nullptr) ? data_->Size() : 0;
    size += (metadata_ != nullptr) ? metadata_->Size() : 0;
    return size;
  }

  /// Whether this object has metadata.
  bool HasMetadata() const { return metadata_ != nullptr && metadata_->Size() > 0; }

 private:
  /// Data of the ray object.
  std::shared_ptr<Buffer> data_;
  /// Metadata of the ray object.
  std::shared_ptr<Buffer> metadata_;
  /// Whether this class holds a data copy.
  bool has_data_copy_;
};

/// Provider interface for store access. Store provider should inherit from this class and
/// provide implementions for the methods. The actual store provider may use a plasma
/// store or local memory store in worker process, or possibly other types of storage.

class CoreWorkerStoreProvider {
 public:
  CoreWorkerStoreProvider() {}

  virtual ~CoreWorkerStoreProvider() {}

  /// Put an object with specified ID into object store.
  ///
  /// \param[in] object The ray object.
  /// \param[in] object_id Object ID specified by user.
  /// \return Status.
  virtual Status Put(const RayObject &object, const ObjectID &object_id) = 0;

  /// Get a set of objects from the object store.
  ///
  /// \param[in] object_ids IDs of the objects to get.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's negative.
  /// \param[in] task_id ID for the current task.
  /// \param[out] results Map of objects to write results into. Get will only add to this
  /// map, not clear or remove from it, so the caller can pass in a non-empty map.
  /// \return Status.
  virtual Status Get(
      const std::unordered_set<ObjectID> &object_ids, int64_t timeout_ms,
      const TaskID &task_id,
      std::unordered_map<ObjectID, std::shared_ptr<RayObject>> *results) = 0;

  /// Wait for a list of objects to appear in the object store. Objects that appear will
  /// be added to the ready set.
  ///
  /// \param[in] object_ids IDs of the objects to wait for.
  /// \param[in] num_objects Number of objects that should appear before returning.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's negative.
  /// \param[in] task_id ID for the current task.
  /// \param[out] ready IDs of objects that have appeared. Wait will only add to this
  /// set, not clear or remove from it, so the caller can pass in a non-empty set.
  /// \return Status.
  virtual Status Wait(const std::unordered_set<ObjectID> &object_ids, int num_objects,
                      int64_t timeout_ms, const TaskID &task_id,
                      std::unordered_set<ObjectID> *ready) = 0;

  /// Delete a list of objects from the object store.
  ///
  /// \param[in] object_ids IDs of the objects to delete.
  /// \param[in] local_only Whether only delete the objects in local node, or all nodes in
  /// the cluster.
  /// \param[in] delete_creating_tasks Whether also delete the tasks that
  /// created these objects.
  /// \return Status.
  virtual Status Delete(const std::vector<ObjectID> &object_ids, bool local_only = true,
                        bool delete_creating_tasks = false) = 0;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_STORE_PROVIDER_H
