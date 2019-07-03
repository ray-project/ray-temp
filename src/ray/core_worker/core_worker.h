#ifndef RAY_CORE_WORKER_CORE_WORKER_H
#define RAY_CORE_WORKER_CORE_WORKER_H

#include "ray/common/buffer.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/object_interface.h"
#include "ray/core_worker/task_execution.h"
#include "ray/core_worker/task_interface.h"
#include "ray/gcs/format/gcs_generated.h"
#include "ray/raylet/raylet_client.h"

namespace ray {

/// The root class that contains all the core and language-independent functionalities
/// of the worker. This class is supposed to be used to implement app-language (Java,
/// Python, etc) workers.
class CoreWorker {
 public:
  /// Construct a CoreWorker instance.
  ///
  /// \param[in] worker_type Type of this worker.
  /// \param[in] langauge Language of this worker.
  ///
  /// NOTE(zhijunfu): the constructor would throw if a failure happens.
  CoreWorker(const WorkerType worker_type, const ::Language language,
             const std::string &store_socket, const std::string &raylet_socket,
             const JobID &job_id = JobID::Nil());

  CoreWorker(const ::Language language, std::shared_ptr<WorkerContext> worker_context,
             std::shared_ptr<CoreWorkerTaskInterface> task_interface,
             std::shared_ptr<CoreWorkerObjectInterface> object_interface,
             std::shared_ptr<CoreWorkerTaskExecutionInterface> task_execution_interface);

  /// Type of this worker.
  enum WorkerType WorkerType() const { return worker_type_; }

  /// Language of this worker.
  ::Language Language() const { return language_; }

  WorkerContext &Context() { return *worker_context_; }

  /// Return the `CoreWorkerTaskInterface` that contains the methods related to task
  /// submisson.
  CoreWorkerTaskInterface &Tasks() { return *task_interface_; }

  /// Return the `CoreWorkerObjectInterface` that contains methods related to object
  /// store.
  CoreWorkerObjectInterface &Objects() { return *object_interface_; }

  /// Return the `CoreWorkerTaskExecutionInterface` that contains methods related to
  /// task execution.
  CoreWorkerTaskExecutionInterface &Execution() { return *task_execution_interface_; }

 private:
  /// Type of this worker.
  const enum WorkerType worker_type_;

  /// Language of this worker.
  const ::Language language_;

  /// Worker context.
  std::shared_ptr<WorkerContext> worker_context_;

  /// The `CoreWorkerTaskInterface` instance.
  std::shared_ptr<CoreWorkerTaskInterface> task_interface_;

  /// The `CoreWorkerObjectInterface` instance.
  std::shared_ptr<CoreWorkerObjectInterface> object_interface_;

  /// The `CoreWorkerTaskExecutionInterface` instance.
  std::shared_ptr<CoreWorkerTaskExecutionInterface> task_execution_interface_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_CORE_WORKER_H
