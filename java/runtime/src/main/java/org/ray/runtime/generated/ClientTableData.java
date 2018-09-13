package org.ray.runtime.generated;
// automatically generated by the FlatBuffers compiler, do not modify

import java.nio.*;
import java.lang.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class ClientTableData extends Table {
  public static ClientTableData getRootAsClientTableData(ByteBuffer _bb) { return getRootAsClientTableData(_bb, new ClientTableData()); }
  public static ClientTableData getRootAsClientTableData(ByteBuffer _bb, ClientTableData obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; }
  public ClientTableData __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public String clientId() { int o = __offset(4); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer clientIdAsByteBuffer() { return __vector_as_bytebuffer(4, 1); }
  public ByteBuffer clientIdInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 4, 1); }
  public String nodeManagerAddress() { int o = __offset(6); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer nodeManagerAddressAsByteBuffer() { return __vector_as_bytebuffer(6, 1); }
  public ByteBuffer nodeManagerAddressInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 6, 1); }
  public String rayletSocketName() { int o = __offset(8); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer rayletSocketNameAsByteBuffer() { return __vector_as_bytebuffer(8, 1); }
  public ByteBuffer rayletSocketNameInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 8, 1); }
  public String objectStoreSocketName() { int o = __offset(10); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer objectStoreSocketNameAsByteBuffer() { return __vector_as_bytebuffer(10, 1); }
  public ByteBuffer objectStoreSocketNameInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 10, 1); }
  public int nodeManagerPort() { int o = __offset(12); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int objectManagerPort() { int o = __offset(14); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public boolean isInsertion() { int o = __offset(16); return o != 0 ? 0!=bb.get(o + bb_pos) : false; }
  public String resourcesTotalLabel(int j) { int o = __offset(18); return o != 0 ? __string(__vector(o) + j * 4) : null; }
  public int resourcesTotalLabelLength() { int o = __offset(18); return o != 0 ? __vector_len(o) : 0; }
  public double resourcesTotalCapacity(int j) { int o = __offset(20); return o != 0 ? bb.getDouble(__vector(o) + j * 8) : 0; }
  public int resourcesTotalCapacityLength() { int o = __offset(20); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer resourcesTotalCapacityAsByteBuffer() { return __vector_as_bytebuffer(20, 8); }
  public ByteBuffer resourcesTotalCapacityInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 20, 8); }

  public static int createClientTableData(FlatBufferBuilder builder,
      int client_idOffset,
      int node_manager_addressOffset,
      int raylet_socket_nameOffset,
      int object_store_socket_nameOffset,
      int node_manager_port,
      int object_manager_port,
      boolean is_insertion,
      int resources_total_labelOffset,
      int resources_total_capacityOffset) {
    builder.startObject(9);
    ClientTableData.addResourcesTotalCapacity(builder, resources_total_capacityOffset);
    ClientTableData.addResourcesTotalLabel(builder, resources_total_labelOffset);
    ClientTableData.addObjectManagerPort(builder, object_manager_port);
    ClientTableData.addNodeManagerPort(builder, node_manager_port);
    ClientTableData.addObjectStoreSocketName(builder, object_store_socket_nameOffset);
    ClientTableData.addRayletSocketName(builder, raylet_socket_nameOffset);
    ClientTableData.addNodeManagerAddress(builder, node_manager_addressOffset);
    ClientTableData.addClientId(builder, client_idOffset);
    ClientTableData.addIsInsertion(builder, is_insertion);
    return ClientTableData.endClientTableData(builder);
  }

  public static void startClientTableData(FlatBufferBuilder builder) { builder.startObject(9); }
  public static void addClientId(FlatBufferBuilder builder, int clientIdOffset) { builder.addOffset(0, clientIdOffset, 0); }
  public static void addNodeManagerAddress(FlatBufferBuilder builder, int nodeManagerAddressOffset) { builder.addOffset(1, nodeManagerAddressOffset, 0); }
  public static void addRayletSocketName(FlatBufferBuilder builder, int rayletSocketNameOffset) { builder.addOffset(2, rayletSocketNameOffset, 0); }
  public static void addObjectStoreSocketName(FlatBufferBuilder builder, int objectStoreSocketNameOffset) { builder.addOffset(3, objectStoreSocketNameOffset, 0); }
  public static void addNodeManagerPort(FlatBufferBuilder builder, int nodeManagerPort) { builder.addInt(4, nodeManagerPort, 0); }
  public static void addObjectManagerPort(FlatBufferBuilder builder, int objectManagerPort) { builder.addInt(5, objectManagerPort, 0); }
  public static void addIsInsertion(FlatBufferBuilder builder, boolean isInsertion) { builder.addBoolean(6, isInsertion, false); }
  public static void addResourcesTotalLabel(FlatBufferBuilder builder, int resourcesTotalLabelOffset) { builder.addOffset(7, resourcesTotalLabelOffset, 0); }
  public static int createResourcesTotalLabelVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startResourcesTotalLabelVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static void addResourcesTotalCapacity(FlatBufferBuilder builder, int resourcesTotalCapacityOffset) { builder.addOffset(8, resourcesTotalCapacityOffset, 0); }
  public static int createResourcesTotalCapacityVector(FlatBufferBuilder builder, double[] data) { builder.startVector(8, data.length, 8); for (int i = data.length - 1; i >= 0; i--) builder.addDouble(data[i]); return builder.endVector(); }
  public static void startResourcesTotalCapacityVector(FlatBufferBuilder builder, int numElems) { builder.startVector(8, numElems, 8); }
  public static int endClientTableData(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
}

