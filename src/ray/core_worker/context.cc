
#include "ray/core_worker/context.h"

namespace ray {

/// per-thread context for core worker.
struct WorkerThreadContext {
  WorkerThreadContext()
      : current_task_id(TaskID::FromRandom()),
        current_actor_id(ActorID::Nil()),
        task_index(0),
        put_index(0) {}

  int GetNextTaskIndex() { return ++task_index; }

  int GetNextPutIndex() { return ++put_index; }

  const TaskID &GetCurrentTaskID() const { return current_task_id; }

  const ActorID &GetCurrentActorID() const { return current_actor_id; }

  void SetCurrentTask(const TaskID &task_id) {
    current_task_id = task_id;
    task_index = 0;
    put_index = 0;
  }

  void SetCurrentTask(const raylet::TaskSpecification &spec) {
    SetCurrentTask(spec.TaskId());
    if (spec.IsActorCreationTask()) {
      RAY_CHECK(current_actor_id.IsNil());
      current_actor_id = spec.ActorCreationId();
    }
    if (spec.IsActorTask()) {
      RAY_CHECK(current_actor_id == spec.ActorId());
    }
  }

 private:
  /// The task ID for current task.
  TaskID current_task_id;

  /// ID of current actor.
  ActorID current_actor_id;

  /// Number of tasks that have been submitted from current task.
  int task_index;

  /// Number of objects that have been put from current task.
  int put_index;
};

thread_local std::unique_ptr<WorkerThreadContext> WorkerContext::thread_context_ =
    nullptr;

WorkerContext::WorkerContext(WorkerType worker_type, const WorkerID &worker_id,
                             const JobID &job_id)
    : worker_type(worker_type),
      // TODO(qwang): Assign the driver id to worker id
      // once we treat driver id as a special worker id.
      worker_id(worker_id),
      current_job_id(job_id) {
  if (worker_type == WorkerType::DRIVER) {
    RAY_CHECK(!job_id.IsNil());
    RAY_CHECK(worker_id == job_id);
  } else {
    RAY_CHECK(!worker_id.IsNil());
    RAY_CHECK(job_id.IsNil());
  }
  // For worker main thread which initializes the WorkerContext,
  // set task_id according to whether current worker is a driver.
  // (For other threads it's set to random ID via GetThreadContext).
  GetThreadContext().SetCurrentTask(
      (worker_type == WorkerType::DRIVER) ? TaskID::FromRandom() : TaskID::Nil());
}

const WorkerType WorkerContext::GetWorkerType() const { return worker_type; }

const WorkerID &WorkerContext::GetWorkerID() const { return worker_id; }

int WorkerContext::GetNextTaskIndex() { return GetThreadContext().GetNextTaskIndex(); }

int WorkerContext::GetNextPutIndex() { return GetThreadContext().GetNextPutIndex(); }

const JobID &WorkerContext::GetCurrentJobID() const { return current_job_id; }

const TaskID &WorkerContext::GetCurrentTaskID() const {
  return GetThreadContext().GetCurrentTaskID();
}

void WorkerContext::SetCurrentTask(const raylet::TaskSpecification &spec) {
  current_job_id = spec.JobId();
  GetThreadContext().SetCurrentTask(spec);
}

const ActorID &WorkerContext::GetCurrentActorID() const {
  return GetThreadContext().GetCurrentActorID();
}

WorkerThreadContext &WorkerContext::GetThreadContext() {
  if (thread_context_ == nullptr) {
    thread_context_ = std::unique_ptr<WorkerThreadContext>(new WorkerThreadContext());
  }

  return *thread_context_;
}

}  // namespace ray
