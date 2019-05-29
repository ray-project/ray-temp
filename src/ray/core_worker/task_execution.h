#ifndef RAY_CORE_WORKER_TASK_EXECUTION_H
#define RAY_CORE_WORKER_TASK_EXECUTION_H

#include "common.h"
#include "ray/common/buffer.h"
#include "ray/status.h"

namespace ray {

class CoreWorker;

namespace raylet {
  class TaskSpecification;
}

/// The interface that contains all `CoreWorker` methods that are related to task
/// execution.
class CoreWorkerTaskExecutionInterface {
 public:
  CoreWorkerTaskExecutionInterface(CoreWorker &core_worker) : core_worker_(core_worker) {}

  /// The callback provided app-language workers that executes tasks.
  ///
  /// \param ray_function[in] Information about the function to execute.
  /// \param args[in] Arguments of the task.
  /// \return Status.
  using TaskExecutor = std::function<Status(const RayFunction &ray_function,
                                            const std::vector<std::shared_ptr<Buffer>> &args)>;

  /// Start receving and executes tasks in a infinite loop.
  void Start(const TaskExecutor &executor);

 private:
  /// Build arguments for task executor. This would loop through all the arguments
  /// in task spec, and for each of them that's passed by reference (ObjectID),
  /// fetch its content from store and; for arguments that are passedby value,
  /// just copy their content. 
  /// 
  /// \param spec[in] Task specification.
  /// \param args[out] The arguments for passing to task executor. 
  /// 
  Status BuildArgsForExecutor(const raylet::TaskSpecification &spec,
      std::vector<std::shared_ptr<Buffer>> *args);

  /// Reference to the parent CoreWorker instance.
  CoreWorker &core_worker_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_TASK_EXECUTION_H
