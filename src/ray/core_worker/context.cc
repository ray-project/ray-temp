
#include "ray/core_worker/context.h"

namespace ray {

/// per-thread context for core worker.
struct WorkerThreadContext {
  WorkerThreadContext()
      : current_task_id_(TaskID::FromRandom()), task_index(0), put_index(0) {}

  int GetNextTaskIndex() { return ++task_index; }

  int GetNextPutIndex() { return ++put_index; }

  const TaskID &GetCurrentTaskID() const { return current_task_id_; }

  std::shared_ptr<const raylet::TaskSpecification> GetCurrentTask() const {
    return current_task_;
  }

  void SetCurrentTaskId(const TaskID &task_id) {
    current_task_id_ = task_id;
    task_index = 0;
    put_index = 0;
  }

  void SetCurrentTask(const raylet::TaskSpecification &spec) {
    SetCurrentTaskId(spec.TaskId());
    current_task_ = std::make_shared<const raylet::TaskSpecification>(spec);
  }

 private:
  /// The task ID for current task.
  TaskID current_task_id_;

  /// The current task.
  std::shared_ptr<const raylet::TaskSpecification> current_task_;

  /// Number of tasks that have been submitted from current task.
  int task_index;

  /// Number of objects that have been put from current task.
  int put_index;
};

thread_local std::unique_ptr<WorkerThreadContext> WorkerContext::thread_context_ =
    nullptr;

WorkerContext::WorkerContext(WorkerType worker_type, const JobID &job_id)
    : worker_type_(worker_type),
      // TODO(qwang): Assign the driver id to worker id
      // once we treat driver id as a special worker id.
      worker_id_(worker_type_ == WorkerType::DRIVER ? ComputeDriverIdFromJob(job_id)
                                                    : WorkerID::FromRandom()),
      current_job_id_(worker_type_ == WorkerType::DRIVER ? job_id : JobID::Nil()) {
  // For worker main thread which initializes the WorkerContext,
  // set task_id according to whether current worker is a driver.
  // (For other threads it's set to random ID via GetThreadContext).
  GetThreadContext().SetCurrentTaskId(
      (worker_type_ == WorkerType::DRIVER) ? TaskID::FromRandom() : TaskID::Nil());
}

const WorkerType WorkerContext::GetWorkerType() const { return worker_type_; }

const WorkerID &WorkerContext::GetWorkerID() const { return worker_id_; }

int WorkerContext::GetNextTaskIndex() { return GetThreadContext().GetNextTaskIndex(); }

int WorkerContext::GetNextPutIndex() { return GetThreadContext().GetNextPutIndex(); }

const JobID &WorkerContext::GetCurrentJobID() const { return current_job_id_; }

const TaskID &WorkerContext::GetCurrentTaskID() const {
  return GetThreadContext().GetCurrentTaskID();
}

void WorkerContext::SetCurrentTask(const raylet::TaskSpecification &spec) {
  current_job_id_ = spec.JobId();
  GetThreadContext().SetCurrentTask(spec);
}

std::shared_ptr<const raylet::TaskSpecification> WorkerContext::GetCurrentTask() const {
  return GetThreadContext().GetCurrentTask();
}

WorkerThreadContext &WorkerContext::GetThreadContext() {
  if (thread_context_ == nullptr) {
    thread_context_ = std::unique_ptr<WorkerThreadContext>(new WorkerThreadContext());
  }

  return *thread_context_;
}

}  // namespace ray
