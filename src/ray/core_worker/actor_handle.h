#ifndef RAY_CORE_WORKER_ACTOR_HANDLE_H
#define RAY_CORE_WORKER_ACTOR_HANDLE_H

#include <gtest/gtest_prod.h>

#include "ray/common/id.h"
#include "ray/common/task/task_util.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/context.h"
#include "ray/protobuf/core_worker.pb.h"

namespace ray {

class ActorHandle {
 public:
  // Constructs a new ActorHandle as part of the actor creation process.
  ActorHandle(const ActorID &actor_id, const ActorHandleID &actor_handle_id,
              const JobID &job_id, const ObjectID &initial_cursor,
              const Language actor_language, bool is_direct_call,
              const std::vector<std::string> &actor_creation_task_function_descriptor);

  /// Constructs an ActorHandle by forking from parent. Note that this will modify
  /// parent to account for the newly forked child handle.
  /// in_band should be set to indicate whether the fork is due to an in-band operation
  /// (e.g., passing the parent actor handle into a remote function) or not, as these
  /// must be treated differently.
  ActorHandle(ActorHandle &parent, bool in_band);

  /// Constructs an ActorHandle from a serialized string.
  ActorHandle(const std::string &serialized, const TaskID &current_task_id);

  ActorID ActorID() const { return ActorID::FromBinary(inner_.actor_id()); };

  ActorHandleID ActorHandleID() const {
    return ActorHandleID::FromBinary(inner_.actor_handle_id());
  };

  /// ID of the job that created the actor (it is possible that the handle
  /// exists on a job with a different job ID).
  JobID CreationJobID() const { return JobID::FromBinary(inner_.creation_job_id()); };

  Language ActorLanguage() const { return inner_.actor_language(); };

  std::vector<std::string> ActorCreationTaskFunctionDescriptor() const {
    return VectorFromProtobuf(inner_.actor_creation_task_function_descriptor());
  };

  ObjectID ActorCursor() const { return ObjectID::FromBinary(inner_.actor_cursor()); }

  bool IsDirectCallActor() const { return inner_.is_direct_call(); }

  void SetActorTaskSpec(TaskSpecBuilder &builder, const TaskTransportType transport_type,
                        const ObjectID new_cursor);

  void Serialize(std::string *output);

 private:
  // Protobuf-defined persistent state of the actor handle.
  ray::rpc::ActorHandle inner_;

  // Number of times this handle has been forked.
  uint64_t num_forks_ = 0;

  // Number of tasks that have been submitted on this handle.
  uint64_t task_counter_ = 0;

  /// The new actor handles that were created from this handle
  /// since the last task on this handle was submitted. This is
  /// used to garbage-collect dummy objects that are no longer
  /// necessary in the backend.
  std::vector<ray::ActorHandleID> new_actor_handles_;

  std::mutex mutex_;

  FRIEND_TEST(ZeroNodeTest, TestActorHandle);
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_ACTOR_HANDLE_H
