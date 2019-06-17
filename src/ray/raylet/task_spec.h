#ifndef RAY_RAYLET_TASK_SPECIFICATION_H
#define RAY_RAYLET_TASK_SPECIFICATION_H

#include <cstddef>
#include <string>
#include <unordered_map>
#include <vector>

#include "ray/common/id.h"
#include "ray/protobuf/common.pb.h"
#include "ray/raylet/scheduling_resources.h"
#include "ray/rpc/message_wrapper.h"

extern "C" {
#include "ray/thirdparty/sha256.h"
}

namespace ray {

namespace raylet {

/// \class TaskSpecification
///
/// The task specification encapsulates all immutable information about the
/// task. These fields are determined at submission time, converse to the
/// TaskExecutionSpecification that may change at execution time.
class TaskSpecification : public rpc::ConstMessageWrapper<rpc::TaskSpec> {
 public:
  /// Deserialize a task specification from a flatbuffer.
  ///
  /// \param string A serialized task specification flatbuffer.
  explicit TaskSpecification(const rpc::TaskSpec &message)
      : ConstMessageWrapper(message) {
    ComputeResources();
  }

  explicit TaskSpecification(std::unique_ptr<const rpc::TaskSpec> message)
      : ConstMessageWrapper(std::move(message)) {
    ComputeResources();
  }
  std::string SerializeAsString() const {
    std::string ret;
    message_->SerializeToString(&ret);
    return ret;
  }

  // TODO(swang): Finalize and document these methods.
  TaskID TaskId() const;

  JobID JobId() const;

  TaskID ParentTaskId() const;

  size_t ParentCounter() const;

  std::vector<std::string> FunctionDescriptor() const;

  // Output the function descriptor as a string for log purpose.
  std::string FunctionDescriptorString() const;

  size_t NumArgs() const;

  size_t NumReturns() const;

  bool ArgByRef(size_t arg_index) const;

  size_t ArgIdCount(size_t arg_index) const;

  ObjectID ArgId(size_t arg_index, size_t id_index) const;

  ObjectID ReturnId(size_t return_index) const;

  const uint8_t *ArgVal(size_t arg_index) const;

  size_t ArgValLength(size_t arg_index) const;

  /// Return the resources that are to be acquired during the execution of this
  /// task.
  ///
  /// \return The resources that will be acquired during the execution of this
  /// task.
  const ResourceSet GetRequiredResources() const;

  /// Return the resources that are required for a task to be placed on a node.
  /// This will typically be the same as the resources acquired during execution
  /// and will always be a superset of those resources. However, they may
  /// differ, e.g., actor creation tasks may require more resources to be
  /// scheduled on a machine because the actor creation task may require no
  /// resources itself, but subsequent actor methods may require resources, and
  /// so the placement of the actor should take this into account.
  ///
  /// \return The resources that are required to place a task on a node.
  const ResourceSet GetRequiredPlacementResources() const;

  bool IsDriverTask() const;

  rpc::Language GetLanguage() const;

  // Methods specific to actor tasks.
  bool IsActorCreationTask() const;

  bool IsActorTask() const;

  ActorID ActorCreationId() const;

  ObjectID ActorCreationDummyObjectId() const;

  uint64_t MaxActorReconstructions() const;

  ActorID ActorId() const;

  ActorHandleID ActorHandleId() const;

  uint64_t ActorCounter() const;

  ObjectID ActorDummyObject() const;

  std::vector<ActorHandleID> NewActorHandles() const;

  std::vector<std::string> DynamicWorkerOptions() const;

 private:
  void ComputeResources();
  /// Field storing required resources. Initalized in constructor.
  ResourceSet required_resources_;
  /// Field storing required placement resources. Initalized in constructor.
  ResourceSet required_placement_resources_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_TASK_SPECIFICATION_H
