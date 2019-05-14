package org.ray.api.id;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

/**
 * Represents a unique id of all Ray concepts, including
 * objects, tasks, workers, actors, etc.
 */
public class ObjectId extends BaseId implements Serializable {

  public static final int LENGTH = 20;
  public static final ObjectId NIL = genNil();

  /**
   * Create a ObjectId from a hex string.
   */
  public static ObjectId fromHexString(String hex) {
    return new ObjectId(hexString2Bytes(hex));
  }

  /**
   * Creates a ObjectId from a ByteBuffer.
   */
  public static ObjectId fromByteBuffer(ByteBuffer bb) {
    return new ObjectId(byteBuffer2Bytes(bb));
  }

  /**
   * Generate a nil ObjectId.
   */
  public static ObjectId genNil() {
    byte[] b = new byte[LENGTH];
    Arrays.fill(b, (byte) 0xFF);
    return new ObjectId(b);
  }

  /**
   * Generate an ObjectId with random value.
   */
  public static ObjectId randomId() {
    byte[] b = new byte[LENGTH];
    new Random().nextBytes(b);
    return new ObjectId(b);
  }

  public ObjectId(byte[] id) {
    super(id);
  }

  @Override
  public int size() {
    return LENGTH;
  }

  /**
   * Create a copy of this ObjectId.
   */
  public ObjectId copy() {
    byte[] nid = Arrays.copyOf(getBytes(), size());
    return new ObjectId(nid);
  }

  public TaskId getTaskId() {
    byte[] taskIdBytes = Arrays.copyOf(getBytes(), TaskId.LENGTH);
    return new TaskId(taskIdBytes);
  }


}
