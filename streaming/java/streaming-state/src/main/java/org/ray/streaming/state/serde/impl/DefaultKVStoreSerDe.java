/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ray.streaming.state.serde.impl;

import org.ray.streaming.state.serde.IKVStoreSerDe;
import org.ray.streaming.state.serde.SerializationHelper;

/**
 * KV Store Serialization and Deserialization.
 */
public class DefaultKVStoreSerDe<K, V> extends AbstractSerDe implements IKVStoreSerDe<K, V> {

  @Override
  public byte[] serKey(K key) {
    String keyWithPrefix = generateRowKeyPrefix(key.toString());
    return keyWithPrefix.getBytes();
  }

  @Override
  public byte[] serValue(V value) {
    return SerializationHelper.object2Byte(value);
  }

  @Override
  public V deSerValue(byte[] valueArray) {
    return (V) SerializationHelper.byte2Object(valueArray);
  }
}
