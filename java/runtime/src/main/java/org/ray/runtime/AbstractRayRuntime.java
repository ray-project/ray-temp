package org.ray.runtime;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.RayPyActor;
import org.ray.api.WaitResult;
import org.ray.api.exception.RayException;
import org.ray.api.function.RayFunc;
import org.ray.api.id.ObjectId;
import org.ray.api.id.UniqueId;
import org.ray.api.options.ActorCreationOptions;
import org.ray.api.options.CallOptions;
import org.ray.api.runtime.RayRuntime;
import org.ray.api.runtimecontext.RuntimeContext;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.context.RuntimeContextImpl;
import org.ray.runtime.context.WorkerContext;
import org.ray.runtime.functionmanager.FunctionDescriptor;
import org.ray.runtime.functionmanager.FunctionManager;
import org.ray.runtime.functionmanager.PyFunctionDescriptor;
import org.ray.runtime.gcs.GcsClient;
import org.ray.runtime.generated.Common.Language;
import org.ray.runtime.object.RayObjectImpl;
import org.ray.runtime.task.ArgumentsBuilder;
import org.ray.runtime.task.FunctionArg;
import org.ray.runtime.task.TaskExecutor;
import org.ray.runtime.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Core functionality to implement Ray APIs.
 */
public abstract class AbstractRayRuntime implements RayRuntime {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRayRuntime.class);
  public static final String PYTHON_INIT_METHOD_NAME = "__init__";
  protected RayConfig rayConfig;
  protected TaskExecutor taskExecutor;
  protected FunctionManager functionManager;
  protected RuntimeContext runtimeContext;
  protected GcsClient gcsClient;

  public AbstractRayRuntime(RayConfig rayConfig) {
    this.rayConfig = rayConfig;
    functionManager = new FunctionManager(rayConfig.jobResourcePath);
    runtimeContext = new RuntimeContextImpl(this);
  }

  /**
   * Start runtime.
   */
  public abstract void start() throws Exception;

  @Override
  public abstract void shutdown();

  @Override
  public <T> RayObject<T> put(T obj) {
    ObjectId objectId = taskExecutor.getObjectStoreProxy().put(obj);
    return new RayObjectImpl<>(objectId);
  }

  @Override
  public <T> T get(ObjectId objectId) throws RayException {
    List<T> ret = get(ImmutableList.of(objectId));
    return ret.get(0);
  }

  @Override
  public <T> List<T> get(List<ObjectId> objectIds) {
    return getTaskExecutor().getObjectStoreProxy().get(objectIds);
  }

  @Override
  public void free(List<ObjectId> objectIds, boolean localOnly, boolean deleteCreatingTasks) {
    taskExecutor.getObjectStoreProxy().delete(objectIds, localOnly, deleteCreatingTasks);
  }

  @Override
  public void setResource(String resourceName, double capacity, UniqueId nodeId) {
    Preconditions.checkArgument(Double.compare(capacity, 0) >= 0);
    if (nodeId == null) {
      nodeId = UniqueId.NIL;
    }
    taskExecutor.getRayletClient().setResource(resourceName, capacity, nodeId);
  }

  @Override
  public <T> WaitResult<T> wait(List<RayObject<T>> waitList, int numReturns, int timeoutMs) {
    Preconditions.checkNotNull(waitList);
    if (waitList.isEmpty()) {
      return new WaitResult<>(Collections.emptyList(), Collections.emptyList());
    }

    List<ObjectId> ids = waitList.stream().map(RayObject::getId).collect(Collectors.toList());

    List<Boolean> ready = taskExecutor.getObjectStoreProxy().wait(ids, numReturns, timeoutMs);
    List<RayObject<T>> readyList = new ArrayList<>();
    List<RayObject<T>> unreadyList = new ArrayList<>();

    for (int i = 0; i < ready.size(); i++) {
      if (ready.get(i)) {
        readyList.add(waitList.get(i));
      } else {
        unreadyList.add(waitList.get(i));
      }
    }

    return new WaitResult<>(readyList, unreadyList);
  }

  @Override
  public RayObject call(RayFunc func, Object[] args, CallOptions options) {
    FunctionDescriptor functionDescriptor =
        functionManager.getFunction(taskExecutor.getWorkerContext().getCurrentJobId(), func)
            .functionDescriptor;
    return callNormalFunction(functionDescriptor, args, options);
  }

  @Override
  public RayObject call(RayFunc func, RayActor<?> actor, Object[] args) {
    FunctionDescriptor functionDescriptor =
        functionManager.getFunction(taskExecutor.getWorkerContext().getCurrentJobId(), func)
            .functionDescriptor;
    return callActorFunction(actor, functionDescriptor, args);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> RayActor<T> createActor(RayFunc actorFactoryFunc,
      Object[] args, ActorCreationOptions options) {
    FunctionDescriptor functionDescriptor =
        functionManager.getFunction(taskExecutor.getWorkerContext().getCurrentJobId(), actorFactoryFunc)
            .functionDescriptor;
    return (RayActor<T>) createActorImpl(functionDescriptor, args, options);
  }

  private void checkPyArguments(Object[] args) {
    for (Object arg : args) {
      Preconditions.checkArgument(
          (arg instanceof RayPyActor) || (arg instanceof byte[]),
          "Python argument can only be a RayPyActor or a byte array, not {}.",
          arg.getClass().getName());
    }
  }

  @Override
  public RayObject callPy(String moduleName, String functionName, Object[] args,
      CallOptions options) {
    checkPyArguments(args);
    PyFunctionDescriptor functionDescriptor = new PyFunctionDescriptor(moduleName, "",
        functionName);
    return callNormalFunction(functionDescriptor, args, options);
  }

  @Override
  public RayObject callPy(RayPyActor pyActor, String functionName, Object... args) {
    checkPyArguments(args);
    PyFunctionDescriptor functionDescriptor = new PyFunctionDescriptor(pyActor.getModuleName(),
        pyActor.getClassName(), functionName);
    return callActorFunction(pyActor, functionDescriptor, args);
  }

  @Override
  public RayPyActor createPyActor(String moduleName, String className, Object[] args,
      ActorCreationOptions options) {
    checkPyArguments(args);
    PyFunctionDescriptor functionDescriptor = new PyFunctionDescriptor(moduleName, className,
        PYTHON_INIT_METHOD_NAME);
    return (RayPyActor) createActorImpl(functionDescriptor, args, options);
  }

  private RayObject callNormalFunction(FunctionDescriptor functionDescriptor,
      Object[] args, CallOptions options) {
    List<FunctionArg> functionArgs = ArgumentsBuilder
        .wrap(args, functionDescriptor.getLanguage() != Language.JAVA);
    List<ObjectId> returnIds = taskExecutor.getTaskSubmitter().submitTask(functionDescriptor,
        functionArgs, 1, options);
    return new RayObjectImpl(returnIds.get(0));
  }

  private RayObject callActorFunction(RayActor rayActor,
      FunctionDescriptor functionDescriptor, Object[] args) {
    List<FunctionArg> functionArgs = ArgumentsBuilder
        .wrap(args, functionDescriptor.getLanguage() != Language.JAVA);
    List<ObjectId> returnIds = taskExecutor.getTaskSubmitter().submitActorTask(rayActor,
        functionDescriptor, functionArgs, 1, null);
    return new RayObjectImpl(returnIds.get(0));
  }

  private RayActor createActorImpl(FunctionDescriptor functionDescriptor,
      Object[] args, ActorCreationOptions options) {
    List<FunctionArg> functionArgs = ArgumentsBuilder
        .wrap(args, functionDescriptor.getLanguage() != Language.JAVA);
    if (functionDescriptor.getLanguage() != Language.JAVA && options != null) {
      Preconditions.checkState(StringUtil.isNullOrEmpty(options.jvmOptions));
    }
    RayActor actor = taskExecutor.getTaskSubmitter()
        .createActor(functionDescriptor, functionArgs,
            options);
    return actor;
  }

  public TaskExecutor getTaskExecutor() {
    return taskExecutor;
  }

  public WorkerContext getWorkerContext() {
    return taskExecutor.getWorkerContext();
  }

  public FunctionManager getFunctionManager() {
    return functionManager;
  }

  public RayConfig getRayConfig() {
    return rayConfig;
  }

  public RuntimeContext getRuntimeContext() {
    return runtimeContext;
  }

  public GcsClient getGcsClient() {
    return gcsClient;
  }
}
