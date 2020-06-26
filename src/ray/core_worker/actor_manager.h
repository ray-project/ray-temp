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

#pragma once

#include "absl/container/flat_hash_map.h"
#include "ray/core_worker/actor_handle.h"
#include "ray/core_worker/reference_count.h"
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/gcs/redis_gcs_client.h"

namespace ray {

/// Class to manage lifetimes of actors that we create (actor children).
/// Currently this class is only used to publish actor DEAD event
/// for actor creation task failures. All other cases are managed
/// by raylet.
class ActorManager {
 public:
  explicit ActorManager(
      std::shared_ptr<gcs::GcsClient> gcs_client,
      std::shared_ptr<CoreWorkerDirectActorTaskSubmitterInterface> direct_actor_submitter,
      std::shared_ptr<ReferenceCounterInterface> reference_counter)
      : gcs_client_(gcs_client),
        direct_actor_submitter_(direct_actor_submitter),
        reference_counter_(reference_counter) {}

  ~ActorManager() {}

  friend class ActorManagerTest;

  /// Register an actor handle.
  ///
  /// This should be called when an actor handle is given to us by another task
  /// or actor. This may be called even if we already have a handle to the same
  /// actor.
  ///
  /// \param[in] actor_handle The actor handle.
  /// \param[in] outer_object_id The object ID that contained the serialized
  /// actor handle, if any.
  /// \param[in] caller_id The caller's task ID
  /// \param[in] call_site The caller's site.
  /// \return The ActorID of the deserialized handle.
  ActorID RegisterActorHandle(std::unique_ptr<ActorHandle> actor_handle,
                              const ObjectID &outer_object_id, const TaskID &caller_id,
                              const std::string &call_site,
                              const rpc::Address &caller_address);

  /// Get a handle to an actor.
  ///
  /// \param[in] actor_id The actor handle to get.
  /// \return reference to the actor_handle's pointer.
  /// NOTE: Returned actorHandle should not be stored anywhere.
  std::unique_ptr<ActorHandle> &GetActorHandle(const ActorID &actor_id);

  /// Check if an actor handle that corresponds to an actor_id exists.
  /// \param[in] actor_id The actor id of a handle.
  /// \return True if the actor_handle for an actor_id exists. False otherwise.
  bool CheckActorHandleExists(const ActorID &actor_id);

  /// Give this worker a handle to an actor.
  ///
  /// This handle will remain as long as the current actor or task is
  /// executing, even if the Python handle goes out of scope. Tasks submitted
  /// through this handle are guaranteed to execute in the same order in which
  /// they are submitted.
  ///
  /// \param actor_handle The handle to the actor.
  /// \param is_owner_handle Whether this is the owner's handle to the actor.
  /// The owner is the creator of the actor and is responsible for telling the
  /// actor to disconnect once all handles are out of scope.
  /// \param[in] caller_id The caller's task ID
  /// \param[in] call_site The caller's site.
  /// \param[in] caller_address The address of the caller.
  /// \return True if the handle was added and False if we already had a handle
  /// to the same actor.
  bool AddActorHandle(std::unique_ptr<ActorHandle> actor_handle, bool is_owner_handle,
                      const TaskID &caller_id, const std::string &call_site,
                      const rpc::Address &caller_address);

  /// Add a callback that is called when an actor goes out of scope.
  ///
  /// \param actor_id The actor id that owns the callback.
  /// \param actor_out_of_scope_callbacks The callback function that will be called when
  /// an actor_id goes out of scope.
  void AddActorOutOfScopeCallback(
      const ActorID &actor_id,
      std::function<void(const ActorID &)> actor_out_of_scope_callbacks);

  /// Get a list of actor_ids from existing actor handles.
  /// This is used for debugging purpose.
  std::vector<ObjectID> GetActorHandleIDsFromHandles();

  /// Resolve locations of actors that are not persisted to GCS yet.
  /// https://github.com/ray-project/ray/pull/8679/files
  /// It is required because core workers persist actor information to
  /// GCS after it resolves all local dependencies. It means that, there's
  /// no way for a worker to figure out if the actor is dead or not, which
  /// can lead a worker to hang forever. 
  void ResolveActorsLocations();

 private:
  /// Handle actor state notification published from GCS.
  ///
  /// \param[in] actor_id The actor id of this notification.
  /// \param[in] actor_data The GCS actor data.
  void HandleActorStateNotification(const ActorID &actor_id,
                                    const gcs::ActorTableData &actor_data);

  /// Get an actor handle.
  ///
  /// \param[in] actor_id The actor handle to get.
  /// \return reference to the actor_handle's pointer.
  std::unique_ptr<ActorHandle> &GetActorHandleInternal(const ActorID &actor_id) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// GCS client
  std::shared_ptr<gcs::GcsClient> gcs_client_;

  /// Interface to submit tasks directly to other actors.
  std::shared_ptr<CoreWorkerDirectActorTaskSubmitterInterface> direct_actor_submitter_;

  /// Used to keep track of actor handle reference counts.
  /// All actor handle related ref counting logic should be included here.
  std::shared_ptr<ReferenceCounterInterface> reference_counter_;

  /// Map from actor ID to a handle to that actor.
  /// Actor handle is a logical abstraction that holds actor handle's states.
  absl::flat_hash_map<ActorID, std::unique_ptr<ActorHandle>> actor_handles_
      GUARDED_BY(mutex_);

  /// Map from actor ID to a callback. Callback is called when
  /// the corresponding handles are gone out of scope.
  absl::flat_hash_map<ActorID, std::function<void(const ActorID &)>>
      actor_out_of_scope_callbacks_ GUARDED_BY(mutex_);

  /// List of actor ids that didn't resolve its location in GCS yet.
  /// This means that these actor information hasn't been persisted to GCS.
  /// It happens only when the actor is not created yet because local dependencies
  /// for actor creation task hasn't been resolved.
  absl::flat_hash_set<ActorID> actors_pending_location_resolution_ GUARDED_BY(mutex_);

  mutable absl::Mutex mutex_;
};

}  // namespace ray
