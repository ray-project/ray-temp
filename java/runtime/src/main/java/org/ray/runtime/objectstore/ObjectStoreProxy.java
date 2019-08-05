package org.ray.runtime.objectstore;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.ray.api.exception.RayActorException;
import org.ray.api.exception.RayException;
import org.ray.api.exception.RayTaskException;
import org.ray.api.exception.RayWorkerException;
import org.ray.api.exception.UnreconstructableException;
import org.ray.api.id.ObjectId;
import org.ray.runtime.RayActorImpl;
import org.ray.runtime.WorkerContext;
import org.ray.runtime.generated.Gcs.ErrorType;
import org.ray.runtime.nativeTypes.NativeRayObject;
import org.ray.runtime.util.Serializer;

/**
 * A class that is used to put/get objects to/from the object store.
 */
public class ObjectStoreProxy {

  private static final byte[] WORKER_EXCEPTION_META = String
      .valueOf(ErrorType.WORKER_DIED.getNumber()).getBytes();
  private static final byte[] ACTOR_EXCEPTION_META = String
      .valueOf(ErrorType.ACTOR_DIED.getNumber()).getBytes();
  private static final byte[] UNRECONSTRUCTABLE_EXCEPTION_META = String
      .valueOf(ErrorType.OBJECT_UNRECONSTRUCTABLE.getNumber()).getBytes();

  private static final byte[] TASK_EXECUTION_EXCEPTION_META = String
      .valueOf(ErrorType.TASK_EXECUTION_EXCEPTION.getNumber()).getBytes();

  private static final byte[] RAW_TYPE_META = "RAW".getBytes();

  private static final byte[] ACTOR_HANDLE_META = "ACTOR_HANDLE".getBytes();

  private final WorkerContext workerContext;

  private final ObjectInterface objectInterface;

  public ObjectStoreProxy(WorkerContext workerContext, ObjectInterface objectInterface) {
    this.workerContext = workerContext;
    this.objectInterface = objectInterface;
  }

  /**
   * Get a list of objects from the object store.
   *
   * @param ids List of the object ids.
   * @param <T> Type of these objects.
   * @return A list of GetResult objects.
   */
  @SuppressWarnings("unchecked")
  public <T> List<T> get(List<ObjectId> ids) {
    // Pass -1 as timeout to wait until all objects are available in object store.
    List<NativeRayObject> dataAndMetaList = objectInterface.get(ids, -1);

    List<T> results = new ArrayList<>();
    for (int i = 0; i < dataAndMetaList.size(); i++) {
      NativeRayObject dataAndMeta = dataAndMetaList.get(i);
      Object object = null;
      if (dataAndMeta != null) {
        object = deserialize(dataAndMeta, ids.get(i));
      }
      if (object instanceof RayException) {
        // If the object is a `RayException`, it means that an error occurred during task
        // execution.
        throw (RayException) object;
      }
      results.add((T) object);
    }
    // This check must be placed after the throw exception statement.
    // Because if there was any exception, The get operation would return early
    // and wouldn't wait until all objects exist.
    Preconditions.checkState(dataAndMetaList.stream().allMatch(Objects::nonNull));
    return results;
  }

  public Object deserialize(NativeRayObject nativeRayObject, ObjectId objectId) {
    byte[] meta = nativeRayObject.metadata;
    byte[] data = nativeRayObject.data;

    // If meta is not null, deserialize the object from meta.
    if (meta != null && meta.length > 0) {
      // If meta is not null, deserialize the object from meta.
      if (Arrays.equals(meta, RAW_TYPE_META)) {
        return data;
      } else if (Arrays.equals(meta, ACTOR_HANDLE_META)) {
        return RayActorImpl.fromBinary(data);
      } else if (Arrays.equals(meta, WORKER_EXCEPTION_META)) {
        return RayWorkerException.INSTANCE;
      } else if (Arrays.equals(meta, ACTOR_EXCEPTION_META)) {
        return RayActorException.INSTANCE;
      } else if (Arrays.equals(meta, UNRECONSTRUCTABLE_EXCEPTION_META)) {
        return new UnreconstructableException(objectId);
      } else if (Arrays.equals(meta, TASK_EXECUTION_EXCEPTION_META)) {
        return Serializer.decode(data, workerContext.getCurrentClassLoader());
      }
      throw new IllegalArgumentException("Unrecognized metadata " + Arrays.toString(meta));
    } else {
      // If data is not null, deserialize the Java object.
      return Serializer.decode(data, workerContext.getCurrentClassLoader());
    }
  }

  /**
   * Serialize an object.
   *
   * @param object The object to serialize.
   * @return The serialized object.
   */
  public NativeRayObject serialize(Object object) {
    if (object instanceof NativeRayObject) {
      return (NativeRayObject) object;
    } else if (object instanceof byte[]) {
      // If the object is a byte array, skip serializing it and use a special metadata to
      // indicate it's raw binary. So that this object can also be read by Python.
      return new NativeRayObject((byte[]) object, RAW_TYPE_META);
    } else if (object instanceof RayActorImpl) {
      return new NativeRayObject(((RayActorImpl) object).fork().Binary(),
          ACTOR_HANDLE_META);
    } else if (object instanceof RayTaskException) {
      return new NativeRayObject(Serializer.encode(object),
          TASK_EXECUTION_EXCEPTION_META);
    } else {
      return new NativeRayObject(Serializer.encode(object), null);
    }
  }

  /**
   * Serialize and put an object to the object store.
   *
   * @param object The object to put.
   * @return Id of the object.
   */
  public ObjectId put(Object object) {
    return objectInterface.put(serialize(object));
  }

  /**
   * Wait for a list of objects to appear in the object store.
   *
   * @param objectIds IDs of the objects to wait for.
   * @param numObjects Number of objects that should appear.
   * @param timeoutMs Timeout in milliseconds, wait infinitely if it's negative.
   * @return A bitset that indicates each object has appeared or not.
   */
  public List<Boolean> wait(List<ObjectId> objectIds, int numObjects, long timeoutMs) {
    return objectInterface.wait(objectIds, numObjects, timeoutMs);
  }

  /**
   * Delete a list of objects from the object store.
   *
   * @param objectIds IDs of the objects to delete.
   * @param localOnly Whether only delete the objects in local node, or all nodes in the cluster.
   * @param deleteCreatingTasks Whether also delete the tasks that created these objects.
   */
  public void delete(List<ObjectId> objectIds, boolean localOnly, boolean deleteCreatingTasks) {
    objectInterface.delete(objectIds, localOnly, deleteCreatingTasks);
  }

  /**
   * Get the object interface.
   *
   * @return The object interface.
   */
  public ObjectInterface getObjectInterface() {
    return objectInterface;
  }
}
