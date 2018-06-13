package org.ray.spi.impl;

// automatically generated by the FlatBuffers compiler, do not modify

import com.google.flatbuffers.FlatBufferBuilder;
import com.google.flatbuffers.Table;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

@SuppressWarnings("unused")
public final class Arg extends Table {
  public static Arg getRootAsArg(ByteBuffer _bb) {
    return getRootAsArg(_bb, new Arg());
  }

  public static Arg getRootAsArg(ByteBuffer _bb, Arg obj) {
    _bb.order(ByteOrder.LITTLE_ENDIAN);
    return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb));
  }

  public Arg __assign(int _i, ByteBuffer _bb) {
    __init(_i, _bb);
    return this;
  }

  public void __init(int _i, ByteBuffer _bb) {
    bb_pos = _i;
    bb = _bb;
  }

  public static int createArg(FlatBufferBuilder builder,
                              int object_idsOffset,
                              int dataOffset) {
    builder.startObject(2);
    Arg.addData(builder, dataOffset);
    Arg.addObjectIds(builder, object_idsOffset);
    return Arg.endArg(builder);
  }

  public static void addData(FlatBufferBuilder builder, int dataOffset) {
    builder.addOffset(1, dataOffset, 0);
  }

  public static void addObjectIds(FlatBufferBuilder builder, int objectIdsOffset) {
    builder.addOffset(0, objectIdsOffset, 0);
  }

  public static int endArg(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }

  public static void startArg(FlatBufferBuilder builder) {
    builder.startObject(2);
  }

  public static int createObjectIdsVector(FlatBufferBuilder builder, int[] data) {
    builder.startVector(4, data.length, 4);
    for (int i = data.length - 1; i >= 0; i--) {
      builder.addOffset(data[i]);
    }
    return builder.endVector();
  }

  public static void startObjectIdsVector(FlatBufferBuilder builder, int numElems) {
    builder.startVector(4, numElems, 4);
  }

  public String objectIds(int j) {
    int o = __offset(4);
    return o != 0 ? __string(__vector(o) + j * 4) : null;
  }

  public int objectIdsLength() {
    int o = __offset(4);
    return o != 0 ? __vector_len(o) : 0;
  }

  public String data() {
    int o = __offset(6);
    return o != 0 ? __string(o + bb_pos) : null;
  }

  public ByteBuffer dataAsByteBuffer() {
    return __vector_as_bytebuffer(6, 1);
  }

  //this is manually added to avoid encoding/decoding cost as our object id is a byte array
  // instead of a string
  public ByteBuffer objectIdAsByteBuffer(int j) {
    int o = __offset(4);
    if (o == 0) {
      return null;
    }

    int offset = __vector(o) + j * 4;
    offset += bb.getInt(offset);
    ByteBuffer src = bb.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    int length = src.getInt(offset);
    src.position(offset + 4);
    src.limit(offset + 4 + length);
    return src;
  }
}


