package io.ray.runtime.gcs;

import com.google.common.base.Preconditions;
import io.ray.api.id.ActorId;
import io.ray.api.id.PlacementGroupId;
import io.ray.api.id.UniqueId;
import java.util.List;

/**
 * `GlobalStateAccessor` is used for accessing information from GCS.
 *
 **/
public class GlobalStateAccessor {
  // NOTE(lingxuan.zlx): this is a singleton, it can not be changed during a Ray session.
  // Native pointer to the C++ GcsStateAccessor.
  private Long globalStateAccessorNativePointer = 0L;
  private static GlobalStateAccessor globalStateAccessor;

  public static synchronized GlobalStateAccessor getInstance(String redisAddress,
                                                             String redisPassword) {
    if (null == globalStateAccessor) {
      globalStateAccessor = new GlobalStateAccessor(redisAddress, redisPassword);
    }
    return globalStateAccessor;
  }

  public static synchronized void destroyInstance() {
    if (null != globalStateAccessor) {
      globalStateAccessor.destroyGlobalStateAccessor();
      globalStateAccessor = null;
    }
  }

  private GlobalStateAccessor(String redisAddress, String redisPassword) {
    globalStateAccessorNativePointer =
      nativeCreateGlobalStateAccessor(redisAddress, redisPassword);
    checkGlobalStateAccessorPointerValid();
    connect();
  }

  private boolean connect() {
    return this.nativeConnect(globalStateAccessorNativePointer);
  }

  private void checkGlobalStateAccessorPointerValid() {
    Preconditions.checkState(globalStateAccessorNativePointer != 0,
        "Global state accessor native pointer must not be 0.");
  }

  /**
   * @return A list of job info with JobInfo protobuf schema.
   */
  public List<byte[]> getAllJobInfo() {
    // Fetch a job list with protobuf bytes format from GCS.
    synchronized (GlobalStateAccessor.class) {
      checkGlobalStateAccessorPointerValid();
      return this.nativeGetAllJobInfo(globalStateAccessorNativePointer);
    }
  }

  /**
   * @return A list of node info with GcsNodeInfo protobuf schema.
   */
  public List<byte[]> getAllNodeInfo() {
    // Fetch a node list with protobuf bytes format from GCS.
    synchronized (GlobalStateAccessor.class) {
      checkGlobalStateAccessorPointerValid();
      return this.nativeGetAllNodeInfo(globalStateAccessorNativePointer);
    }
  }

  /**
   * @param nodeId node unique id.
   * @return A map of node resource info in protobuf schema.
   */
  public byte[] getNodeResourceInfo(UniqueId nodeId) {
    synchronized (GlobalStateAccessor.class) {
      checkGlobalStateAccessorPointerValid();
      return nativeGetNodeResourceInfo(globalStateAccessorNativePointer, nodeId.getBytes());
    }
  }

  public byte[] getPlacementGroupInfo(PlacementGroupId placementGroupId) {
    synchronized (GlobalStateAccessor.class) {
      Preconditions.checkNotNull(placementGroupId,
          "PlacementGroupId can't be null when get placement group info.");
      return nativeGetPlacementGroupInfo(globalStateAccessorNativePointer,
        placementGroupId.getBytes());
    }
  }

  public List<byte[]> getAllPlacementGroupInfo() {
    synchronized (GlobalStateAccessor.class) {
      checkGlobalStateAccessorPointerValid();
      return this.nativeGetAllPlacementGroupInfo(globalStateAccessorNativePointer);
    }
  }

  public byte[] getInternalConfig() {
    synchronized (GlobalStateAccessor.class) {
      checkGlobalStateAccessorPointerValid();
      return nativeGetInternalConfig(globalStateAccessorNativePointer);
    }
  }

  /**
   * @return A list of actor info with ActorInfo protobuf schema.
   */
  public List<byte[]> getAllActorInfo() {
    // Fetch a actor list with protobuf bytes format from GCS.
    synchronized (GlobalStateAccessor.class) {
      checkGlobalStateAccessorPointerValid();
      return this.nativeGetAllActorInfo(globalStateAccessorNativePointer);
    }
  }

  /**
   * @return An actor info with ActorInfo protobuf schema.
   */
  public byte[] getActorInfo(ActorId actorId) {
    // Fetch an actor with protobuf bytes format from GCS.
    synchronized (GlobalStateAccessor.class) {
      checkGlobalStateAccessorPointerValid();
      return this.nativeGetActorInfo(globalStateAccessorNativePointer, actorId.getBytes());
    }
  }

  /**
   * @return An actor checkpoint id data with ActorCheckpointIdData protobuf schema.
   */
  public byte[] getActorCheckpointId(ActorId actorId) {
    // Fetch an actor checkpoint id with protobuf bytes format from GCS.
    synchronized (GlobalStateAccessor.class) {
      checkGlobalStateAccessorPointerValid();
      return this.nativeGetActorCheckpointId(globalStateAccessorNativePointer, actorId.getBytes());
    }
  }

  private void destroyGlobalStateAccessor() {
    synchronized (GlobalStateAccessor.class) {
      if (0 == globalStateAccessorNativePointer) {
        return;
      }
      this.nativeDestroyGlobalStateAccessor(globalStateAccessorNativePointer);
      globalStateAccessorNativePointer = 0L;
    }
  }

  private native long nativeCreateGlobalStateAccessor(String redisAddress, String redisPassword);

  private native void nativeDestroyGlobalStateAccessor(long nativePtr);

  private native boolean nativeConnect(long nativePtr);

  private native List<byte[]> nativeGetAllJobInfo(long nativePtr);

  private native List<byte[]> nativeGetAllNodeInfo(long nativePtr);

  private native byte[] nativeGetNodeResourceInfo(long nativePtr, byte[] nodeId);

  private native byte[] nativeGetInternalConfig(long nativePtr);

  private native List<byte[]> nativeGetAllActorInfo(long nativePtr);

  private native byte[] nativeGetActorInfo(long nativePtr, byte[] actorId);

  private native byte[] nativeGetActorCheckpointId(long nativePtr, byte[] actorId);

  private native byte[] nativeGetPlacementGroupInfo(long nativePtr,
      byte[] placementGroupId);

  private native List<byte[]> nativeGetAllPlacementGroupInfo(long nativePtr);
}
