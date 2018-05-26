package org.ray.spi.impl;

// automatically generated by the FlatBuffers compiler, do not modify

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class ResourcePair extends Table {
  public static ResourcePair getRootAsResourcePair(ByteBuffer _bb) { return getRootAsResourcePair(_bb, new ResourcePair()); }
  public static ResourcePair getRootAsResourcePair(ByteBuffer _bb, ResourcePair obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; }
  public ResourcePair __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public String key() { int o = __offset(4); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer keyAsByteBuffer() { return __vector_as_bytebuffer(4, 1); }
  public double value() { int o = __offset(6); return o != 0 ? bb.getDouble(o + bb_pos) : 0.0; }

  public static int createResourcePair(FlatBufferBuilder builder,
      int keyOffset,
      double value) {
    builder.startObject(2);
    ResourcePair.addValue(builder, value);
    ResourcePair.addKey(builder, keyOffset);
    return ResourcePair.endResourcePair(builder);
  }

  public static void startResourcePair(FlatBufferBuilder builder) { builder.startObject(2); }
  public static void addKey(FlatBufferBuilder builder, int keyOffset) { builder.addOffset(0, keyOffset, 0); }
  public static void addValue(FlatBufferBuilder builder, double value) { builder.addDouble(1, value, 0.0); }
  public static int endResourcePair(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
}

