#ifndef RAY_CORE_WORKER_TASK_PROVIDER_H
#define RAY_CORE_WORKER_TASK_PROVIDER_H

#include <list>

#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/core_worker/common.h"
#include "ray/raylet/task_spec.h"

namespace ray {

/// Provider interface for task submission and execution. They are separate classes
/// but should be used in pairs - one type of task submission provider should be used
/// together with task execution provider with the same type, so these classes are
/// put together in this same file.
///
/// Task submission/execution provider should inherit from these classes and provide
/// implementions for the methods. The actual task provider can submit/get tasks via
/// raylet, or directly to/from another worker.

class CoreWorkerTaskSubmissionProvider {
 public:
  CoreWorkerTaskSubmissionProvider() {}

  /// Submit a task for execution.
  ///
  /// \param[in] task The task spec to submit.
  /// \return Status.
  virtual Status SubmitTask(const TaskSpec &task) = 0;
};

class CoreWorkerTaskExecutionProvider {
 public:
  CoreWorkerTaskExecutionProvider() {}

  // Get tasks for execution.
  virtual Status GetTasks(std::vector<TaskSpec> *tasks) = 0;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_TASK_PROVIDER_H
