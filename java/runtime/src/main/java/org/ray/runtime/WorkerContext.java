package org.ray.runtime;

import org.ray.api.id.UniqueId;
import org.ray.runtime.task.TaskInfo;
import org.ray.runtime.util.RayObjectValueConverter;

public class WorkerContext {
  private final long nativeCoreWorker;

  private ClassLoader currentClassLoader;

  private RayObjectValueConverter rayObjectValueConverter = new RayObjectValueConverter(null);

  private TaskInfo currentTask;

  public WorkerContext(long nativeCoreWorker) {
    this.nativeCoreWorker = nativeCoreWorker;
  }

  public RayObjectValueConverter getRayObjectValueConverter() {
    return rayObjectValueConverter;
  }

  /**
   * The ID of the current job.
   */
  public UniqueId getCurrentJobId() {
    return new UniqueId(getCurrentJobId(nativeCoreWorker));
  }

  /**
   * @return The ID of the current worker.
   */
  public UniqueId getCurrentActorId() {
    return new UniqueId(getCurrentActorId(nativeCoreWorker));
  }

  public UniqueId getCurrentWorkerId() {
    return new UniqueId(getCurrentWorkerId(nativeCoreWorker));
  }

  /**
   * @return The class loader which is associated with the current job.
   */
  public ClassLoader getCurrentClassLoader() {
    return currentClassLoader;
  }

  public void setCurrentClassLoader(ClassLoader currentClassLoader) {
    if (this.currentClassLoader != currentClassLoader) {
      this.currentClassLoader = currentClassLoader;
      rayObjectValueConverter = new RayObjectValueConverter(currentClassLoader);
    }
  }

  /**
   * Get the current task.
   */
  public TaskInfo getCurrentTask() {
    return currentTask;
  }

  public void setCurrentTask(TaskInfo currentTask) {
    this.currentTask = currentTask;
  }

  private static native byte[] getCurrentJobId(long nativeCoreWorker);

  private static native byte[] getCurrentWorkerId(long nativeCoreWorker);

  private static native byte[] getCurrentActorId(long nativeCoreWorker);
}
