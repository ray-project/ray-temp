package org.ray.runtime.raylet;

import java.nio.ByteBuffer;
import org.ray.api.exception.RayException;
import org.ray.api.id.ActorId;
import org.ray.api.id.UniqueId;
import org.ray.api.id.ObjectId;

public class RayletClientImpl implements RayletClient {

  /**
   * The native pointer of core worker.
   */
  private long nativeCoreWorkerPointer = 0;

  public RayletClientImpl(long nativeCoreWorkerPointer) {
    this.nativeCoreWorkerPointer = nativeCoreWorkerPointer;
  }

  @Override
  public UniqueId prepareCheckpoint(ActorId actorId) {
    return new UniqueId(nativePrepareCheckpoint(nativeCoreWorkerPointer, actorId.getBytes()));
  }

  @Override
  public void notifyActorResumedFromCheckpoint(ActorId actorId, UniqueId checkpointId) {
    nativeNotifyActorResumedFromCheckpoint(nativeCoreWorkerPointer, actorId.getBytes(),
        checkpointId.getBytes());
  }


  public void setResource(String resourceName, double capacity, UniqueId nodeId) {
    nativeSetResource(nativeCoreWorkerPointer, resourceName, capacity, nodeId.getBytes());
  }

  /// Native method declarations.
  ///
  /// If you change the signature of any native methods, please re-generate
  /// the C++ header file and update the C++ implementation accordingly:
  ///
  /// Suppose that $Dir is your ray root directory.
  /// 1) pushd $Dir/java/runtime/target/classes
  /// 2) javah -classpath .:$Dir/java/api/target/classes org.ray.runtime.raylet.RayletClientImpl
  /// 3) clang-format -i org_ray_runtime_raylet_RayletClientImpl.h
  /// 4) cp org_ray_runtime_raylet_RayletClientImpl.h $Dir/src/ray/raylet/lib/java/
  /// 5) vim $Dir/src/ray/raylet/lib/java/org_ray_runtime_raylet_RayletClientImpl.cc
  /// 6) popd

  private static native byte[] nativePrepareCheckpoint(long conn, byte[] actorId);

  private static native void nativeNotifyActorResumedFromCheckpoint(long conn, byte[] actorId,
      byte[] checkpointId);

  private static native void nativeSetResource(long conn, String resourceName, double capacity,
      byte[] nodeId) throws RayException;
}
