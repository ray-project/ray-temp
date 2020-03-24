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

package org.ray.streaming.state.backend;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.ray.streaming.state.TransactionStateStoreManager;
import org.ray.streaming.state.keystate.desc.AbstractStateDescriptor;
import org.ray.streaming.state.keystate.desc.AbstractStateDescriptor.DescType;
import org.ray.streaming.state.keystate.state.proxy.ListStateStoreManagerProxy;
import org.ray.streaming.state.keystate.state.proxy.MapStateStoreManagerProxy;
import org.ray.streaming.state.keystate.state.proxy.ValueStateStoreManagerProxy;
import org.ray.streaming.state.store.IKeyMapStore;
import org.ray.streaming.state.store.IKVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transaction support primitive operations like finish, commit, ackcommit and rollback.
 * <p>
 * State VALUE modification is not thread safe! By default, every processing thread has its own
 * space to handle state.
 */
public abstract class TransactionKeyStateBackend implements TransactionStateStoreManager {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionKeyStateBackend.class);

  protected long currentCheckpointId;
  protected Object currentKey;
  protected int keyGroupIndex = -1;
  protected Map<String, ValueStateStoreManagerProxy> valueManagerProxyHashMap = new HashMap<>();
  protected Map<String, ListStateStoreManagerProxy> listManagerProxyHashMap = new HashMap<>();
  protected Map<String, MapStateStoreManagerProxy> mapManagerProxyHashMap = new HashMap<>();
  protected Set<String> descNamespace;

  /**
   * tablename, IKVStore key, checkpointId, content
   */
  protected Map<String, IKVStore<String, Map<Long, byte[]>>> backStorageCache;
  private AbstractStateBackend backend;

  public TransactionKeyStateBackend(AbstractStateBackend backend) {
    this.backStorageCache = new HashMap<>();
    this.backend = backend;
    this.descNamespace = new HashSet<>();
  }

  public <K, T> void put(AbstractStateDescriptor descriptor, K key, T value) {
    String desc = descriptor.getIdentify();
    if (descriptor.getDescType() == DescType.VALUE) {
      if (this.valueManagerProxyHashMap.containsKey(desc)) {
        valueManagerProxyHashMap.get(desc).put((String) key, value);
      }
    } else if (descriptor.getDescType() == DescType.LIST) {
      if (this.listManagerProxyHashMap.containsKey(desc)) {
        listManagerProxyHashMap.get(desc).put((String) key, value);
      }
    } else if (descriptor.getDescType() == DescType.MAP) {
      if (this.mapManagerProxyHashMap.containsKey(desc)) {
        mapManagerProxyHashMap.get(desc).put((String) key, value);
      }
    }
  }

  public <K, T> T get(AbstractStateDescriptor descriptor, K key) {
    String desc = descriptor.getIdentify();
    if (descriptor.getDescType() == DescType.VALUE) {
      if (this.valueManagerProxyHashMap.containsKey(desc)) {
        return (T) valueManagerProxyHashMap.get(desc).get((String) key);
      }
    } else if (descriptor.getDescType() == DescType.LIST) {
      if (this.listManagerProxyHashMap.containsKey(desc)) {
        return (T) listManagerProxyHashMap.get(desc).get((String) key);
      }
    } else if (descriptor.getDescType() == DescType.MAP) {
      if (this.mapManagerProxyHashMap.containsKey(desc)) {
        return (T) mapManagerProxyHashMap.get(desc).get((String) key);
      }
    }
    return null;
  }

  @Override
  public void finish(long checkpointId) {
    for (Entry<String, ValueStateStoreManagerProxy> entry : valueManagerProxyHashMap.entrySet()) {
      entry.getValue().finish(checkpointId);
    }
    for (Entry<String, ListStateStoreManagerProxy> entry : listManagerProxyHashMap.entrySet()) {
      entry.getValue().finish(checkpointId);
    }
    for (Entry<String, MapStateStoreManagerProxy> entry : mapManagerProxyHashMap.entrySet()) {
      entry.getValue().finish(checkpointId);
    }
  }

  @Override
  public void commit(long checkpointId) {
    for (Entry<String, ValueStateStoreManagerProxy> entry : valueManagerProxyHashMap.entrySet()) {
      entry.getValue().commit(checkpointId);
    }
    for (Entry<String, ListStateStoreManagerProxy> entry : listManagerProxyHashMap.entrySet()) {
      entry.getValue().commit(checkpointId);
    }
    for (Entry<String, MapStateStoreManagerProxy> entry : mapManagerProxyHashMap.entrySet()) {
      entry.getValue().commit(checkpointId);
    }
  }

  @Override
  public void ackCommit(long checkpointId, long timeStamp) {
    for (Entry<String, ValueStateStoreManagerProxy> entry : valueManagerProxyHashMap.entrySet()) {
      entry.getValue().ackCommit(checkpointId, timeStamp);
    }
    for (Entry<String, ListStateStoreManagerProxy> entry : listManagerProxyHashMap.entrySet()) {
      entry.getValue().ackCommit(checkpointId, timeStamp);
    }
    for (Entry<String, MapStateStoreManagerProxy> entry : mapManagerProxyHashMap.entrySet()) {
      entry.getValue().ackCommit(checkpointId, timeStamp);
    }
  }

  @Override
  public void rollBack(long checkpointId) {
    for (Entry<String, ValueStateStoreManagerProxy> entry : valueManagerProxyHashMap.entrySet()) {
      LOG.warn("backend rollback:{},{}", entry.getKey(), checkpointId);
      entry.getValue().rollBack(checkpointId);
    }
    for (Entry<String, ListStateStoreManagerProxy> entry : listManagerProxyHashMap.entrySet()) {
      LOG.warn("backend rollback:{},{}", entry.getKey(), checkpointId);
      entry.getValue().rollBack(checkpointId);
    }
    for (Entry<String, MapStateStoreManagerProxy> entry : mapManagerProxyHashMap.entrySet()) {
      LOG.warn("backend rollback:{},{}", entry.getKey(), checkpointId);
      entry.getValue().rollBack(checkpointId);
    }
  }

  public IKVStore<String, Map<Long, byte[]>> getBackStorage(String tableName) {
    if (this.backStorageCache.containsKey(tableName)) {
      return this.backStorageCache.get(tableName);
    } else {
      IKeyMapStore<String, Long, byte[]> ikvStore = this.backend.getKeyMapStore(tableName);
      this.backStorageCache.put(tableName, ikvStore);
      return ikvStore;
    }
  }

  public IKVStore<String, Map<Long, byte[]>> getBackStorage(
      AbstractStateDescriptor stateDescriptor) {
    String tableName = this.backend.getTableName(stateDescriptor);
    return getBackStorage(tableName);
  }

  public StateStrategy getStateStrategyEnum() {
    return this.backend.getStateStrategy();
  }

  public BackendType getBackendType() {
    return this.backend.getBackendType();
  }

  public Object getCurrentKey() {
    return this.currentKey;
  }

  public abstract void setCurrentKey(Object currentKey);

  public long getCheckpointId() {
    return this.currentCheckpointId;
  }

  public void setCheckpointId(long checkpointId) {
    this.currentCheckpointId = checkpointId;
  }

  public void setContext(long checkpointId, Object currentKey) {
    setCheckpointId(checkpointId);
    setCurrentKey(currentKey);
  }

  public AbstractStateBackend getBackend() {
    return backend;
  }

  public int getKeyGroupIndex() {
    return this.keyGroupIndex;
  }

  public void setKeyGroupIndex(int keyGroupIndex) {
    this.keyGroupIndex = keyGroupIndex;
  }
}
