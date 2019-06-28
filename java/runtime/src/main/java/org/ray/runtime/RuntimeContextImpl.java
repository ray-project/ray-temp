package org.ray.runtime;

import com.google.common.base.Preconditions;
import java.util.List;
import org.ray.api.id.UniqueId;
import org.ray.api.runtimecontext.NodeInfo;
import org.ray.api.runtimecontext.RuntimeContext;
import org.ray.runtime.config.RunMode;
import org.ray.runtime.task.TaskInfo;

public class RuntimeContextImpl implements RuntimeContext {

  private AbstractRayRuntime runtime;

  public RuntimeContextImpl(AbstractRayRuntime runtime) {
    this.runtime = runtime;
  }

  @Override
  public UniqueId getCurrentJobId() {
    return runtime.getWorker().getWorkerContext().getCurrentJobId();
  }

  @Override
  public UniqueId getCurrentActorId() {
    Worker worker = runtime.getWorker();
    Preconditions.checkState(worker != null);
    UniqueId actorId = worker.getWorkerContext().getCurrentActorId();
    Preconditions.checkState(actorId != null && !actorId.isNil(),
        "This method should only be called from an actor.");
    return actorId;
  }

  @Override
  public boolean wasCurrentActorReconstructed() {
    TaskInfo currentTask = runtime.getWorker().getWorkerContext().getCurrentTask();
    Preconditions.checkState(currentTask != null && currentTask.isActorCreationTask(),
        "This method can only be called from an actor creation task.");
    if (isSingleProcess()) {
      return false;
    }

    return runtime.getGcsClient().actorExists(getCurrentActorId());
  }

  @Override
  public String getRayletSocketName() {
    return runtime.getRayConfig().rayletSocketName;
  }

  @Override
  public String getObjectStoreSocketName() {
    return runtime.getRayConfig().objectStoreSocketName;
  }

  @Override
  public boolean isSingleProcess() {
    return RunMode.SINGLE_PROCESS == runtime.getRayConfig().runMode;
  }

  @Override
  public List<NodeInfo> getAllNodeInfo() {
    return runtime.getGcsClient().getAllNodeInfo();
  }
}
