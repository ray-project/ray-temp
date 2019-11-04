#ifndef RAY_CORE_WORKER_REF_COUNT_H
#define RAY_CORE_WORKER_REF_COUNT_H

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"

#include "ray/common/id.h"
#include "ray/util/logging.h"

namespace ray {

/// Class used by the core worker to keep track of ObjectID reference counts for garbage
/// collection. This class is thread safe.
class ReferenceCounter {
 public:
  ReferenceCounter() {}

  ~ReferenceCounter() {}

  /// Increase the reference count for the ObjectID by num_references. If there is no
  /// entry for the ObjectID, one will be created with no dependencies.
  void AddReference(const ObjectID &object_id, size_t num_references = 1)
      LOCKS_EXCLUDED(mutex_);

  /// Decrease the reference count for the ObjectID by one. If the reference count reaches
  /// zero, it will be erased from the map and the reference count for all of its
  /// dependencies will be decreased be one.
  void RemoveReference(const ObjectID &object_id) LOCKS_EXCLUDED(mutex_);

  /// Set the dependencies for the ObjectID. Dependencies for each ObjectID must only be
  /// set once. The direct reference count for the ObjectID is set to zero.
  void SetDependencies(const ObjectID &object_id,
                       std::shared_ptr<std::vector<ObjectID>> dependencies)
      LOCKS_EXCLUDED(mutex_);

  /// Returns a set of all ObjectIDs currently in scope (i.e., nonzero reference count).
  std::unordered_set<ObjectID> GetAllInScopeObjectIDs() LOCKS_EXCLUDED(mutex_);

  /// Dumps information about all currently tracked references to RAY_LOG(DEBUG).
  void LogDebugString() LOCKS_EXCLUDED(mutex_);

 private:
  /// Recursive helper function for decreasing reference counts. Will recursively call
  /// itself on any dependencies whose reference count reaches zero as a result of
  /// removing the reference.
  void RemoveReferenceRecursive(const ObjectID &object_id)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Protects access to the reference counting state.
  absl::Mutex mutex_;

  /// Holds all direct reference counts and dependency information for tracked ObjectIDs.
  absl::flat_hash_map<ObjectID, std::pair<size_t, std::shared_ptr<std::vector<ObjectID>>>>
      object_id_refs_ GUARDED_BY(mutex_);
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_REF_COUNT_H
