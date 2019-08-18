package org.ray.api.id;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;

/**
 * Represents the id of a Ray task.
 */
public class TaskId extends BaseId implements Serializable {

  public static final int LENGTH = 14;

  public static final TaskId NIL = genNil();

  /**
   * Create a TaskId from a hex string.
   */
  public static TaskId fromHexString(String hex) {
    return new TaskId(hexString2Bytes(hex));
  }

  /**
   * Creates a TaskId from a ByteBuffer.
   */
  public static TaskId fromByteBuffer(ByteBuffer bb) {
    return new TaskId(byteBuffer2Bytes(bb));
  }

  /**
   * Creates a TaskId from given bytes.
   */
  public static TaskId fromBytes(byte[] bytes) {
    return new TaskId(bytes);
  }

  /**
   * Generate a nil TaskId.
   */
  private static TaskId genNil() {
    byte[] b = new byte[LENGTH];
    Arrays.fill(b, (byte) 0xFF);
    return new TaskId(b);
  }

  private TaskId(byte[] id) {
    super(id);
  }

  @Override
  public int size() {
    return LENGTH;
  }
}
