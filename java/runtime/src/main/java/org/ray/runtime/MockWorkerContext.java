package org.ray.runtime;

import com.google.common.base.Preconditions;
import org.ray.api.id.ActorId;
import org.ray.api.id.JobId;
import org.ray.api.id.TaskId;
import org.ray.api.id.UniqueId;
import org.ray.runtime.generated.Common.TaskSpec;
import org.ray.runtime.generated.Common.TaskType;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class MockWorkerContext implements WorkerContext {

  private final JobId jobId;
  private ThreadLocal<TaskSpec> currentTask = new ThreadLocal<>();
  private ClassLoader currentClassLoader;

  public MockWorkerContext(JobId jobId) {
    this.jobId = jobId;
  }

  @Override
  public UniqueId getCurrentWorkerId() {
    throw new NotImplementedException();
  }

  @Override
  public JobId getCurrentJobId() {
    return jobId;
  }

  @Override
  public ActorId getCurrentActorId() {
    TaskSpec taskSpec = currentTask.get();
    if (taskSpec == null) {
      return ActorId.NIL;
    }
    return MockTaskInterface.getActorId(taskSpec);
  }

  @Override
  public ClassLoader getCurrentClassLoader() {
    return currentClassLoader;
  }

  @Override
  public void setCurrentClassLoader(ClassLoader currentClassLoader) {
    this.currentClassLoader = currentClassLoader;
  }

  @Override
  public TaskType getCurrentTaskType() {
    TaskSpec taskSpec = currentTask.get();
    Preconditions.checkNotNull(taskSpec, "Current task is not set.");
    return taskSpec.getType();
  }

  @Override
  public TaskId getCurrentTaskId() {
    TaskSpec taskSpec = currentTask.get();
    Preconditions.checkState(taskSpec != null);
    return TaskId.fromBytes(taskSpec.getTaskId().toByteArray());
  }

  public void setCurrentTask(TaskSpec taskSpec) {
    currentTask.set(taskSpec);
  }
}
