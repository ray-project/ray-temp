package org.ray.runtime;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.ray.api.RayActor;
import org.ray.api.id.ObjectId;
import org.ray.api.options.ActorCreationOptions;
import org.ray.api.options.CallOptions;
import org.ray.runtime.functionmanager.FunctionDescriptor;
import org.ray.runtime.nativeTypes.NativeTaskArg;
import org.ray.runtime.task.FunctionArg;

public class TaskInterfaceImpl implements TaskInterface {

  /**
   * The native pointer of core worker.
   */
  private final long nativeCoreWorkerPointer;

  public TaskInterfaceImpl(long nativeCoreWorkerPointer) {
    this.nativeCoreWorkerPointer = nativeCoreWorkerPointer;
  }

  @Override
  public List<ObjectId> submitTask(FunctionDescriptor functionDescriptor, FunctionArg[] args,
      int numReturns, CallOptions options) {
    List<NativeTaskArg> nativeArgs =
        Arrays.stream(args).map(NativeTaskArg::new).collect(Collectors.toList());
    List<byte[]> returnIds = nativeSubmitTask(nativeCoreWorkerPointer, functionDescriptor,
        nativeArgs, numReturns, options);
    return returnIds.stream().map(ObjectId::new).collect(Collectors.toList());
  }

  @Override
  public RayActor createActor(FunctionDescriptor functionDescriptor, FunctionArg[] args,
      ActorCreationOptions options) {
    List<NativeTaskArg> nativeArgs =
        Arrays.stream(args).map(NativeTaskArg::new).collect(Collectors.toList());
    long nativeActorHandle = nativeCreateActor(nativeCoreWorkerPointer,
        functionDescriptor, nativeArgs, options);
    return new RayActorImpl(nativeActorHandle);
  }

  @Override
  public List<ObjectId> submitActorTask(RayActor actor, FunctionDescriptor functionDescriptor,
      FunctionArg[] args, int numReturns, CallOptions options) {
    Preconditions.checkState(actor instanceof RayActorImpl);
    List<NativeTaskArg> nativeArgs =
        Arrays.stream(args).map(NativeTaskArg::new).collect(Collectors.toList());
    List<byte[]> returnIds = nativeSubmitActorTask(nativeCoreWorkerPointer,
        ((RayActorImpl) actor).getNativeActorHandle(),
        functionDescriptor, nativeArgs, numReturns, options);
    return returnIds.stream().map(ObjectId::new).collect(Collectors.toList());
  }

  private static native List<byte[]> nativeSubmitTask(long nativeCoreWorkerPointer,
      FunctionDescriptor functionDescriptor, List<NativeTaskArg> args, int numReturns,
      CallOptions callOptions);

  private static native long nativeCreateActor(long nativeCoreWorkerPointer,
      FunctionDescriptor functionDescriptor, List<NativeTaskArg> args,
      ActorCreationOptions actorCreationOptions);

  private static native List<byte[]> nativeSubmitActorTask(long nativeCoreWorkerPointer,
      long nativeActorHandle, FunctionDescriptor functionDescriptor, List<NativeTaskArg> args,
      int numReturns, CallOptions callOptions);
}
