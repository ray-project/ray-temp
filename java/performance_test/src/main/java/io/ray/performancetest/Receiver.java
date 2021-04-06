package io.ray.performancetest;

import java.nio.ByteBuffer;

public class Receiver {

  private static final String RECEIVER_RECEIVE_QPS = "receiver.receive.qps";

  private int value = 0;

  public Receiver(String jobName) {}

  public boolean ping() {
    return true;
  }

  public void noArgsNoReturn() {
    value += 1;
  }

  public int noArgsHasReturn() {
    value += 1;
    return value;
  }

  public void bytesNoReturn(byte[] data) {
    value += 1;
  }

  public int bytesHasReturn(byte[] data) {
    value += 1;
    return value;
  }

  public void byteBufferNoReturn(ByteBuffer data) {
    value += 1;
  }

  public int byteBufferHasReturn(ByteBuffer data) {
    value += 1;
    return value;
  }
}
