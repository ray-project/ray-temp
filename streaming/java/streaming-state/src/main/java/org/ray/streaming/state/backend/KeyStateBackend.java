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

import org.ray.streaming.state.keystate.KeyGroup;
import org.ray.streaming.state.keystate.KeyGroupAssignment;
import org.ray.streaming.state.keystate.desc.ListStateDescriptor;
import org.ray.streaming.state.keystate.desc.MapStateDescriptor;
import org.ray.streaming.state.keystate.desc.ValueStateDescriptor;
import org.ray.streaming.state.keystate.state.ListState;
import org.ray.streaming.state.keystate.state.MapState;
import org.ray.streaming.state.keystate.state.ValueState;
import org.ray.streaming.state.keystate.state.proxy.ListStateStoreManagerProxy;
import org.ray.streaming.state.keystate.state.proxy.MapStateStoreManagerProxy;
import org.ray.streaming.state.keystate.state.proxy.ValueStateStoreManagerProxy;

/**
 * key state backend manager, managing different kinds of states.
 */
public class KeyStateBackend extends AbstractKeyStateBackend {

  protected final int numberOfKeyGroups;
  protected final KeyGroup keyGroup;

  public KeyStateBackend(int numberOfKeyGroups, KeyGroup keyGroup,
                         AbstractStateBackend abstractStateBackend) {
    super(abstractStateBackend);
    this.numberOfKeyGroups = numberOfKeyGroups;
    this.keyGroup = keyGroup;
  }

  /**
   * value State
   */
  protected <T> ValueStateStoreManagerProxy<T> newValueStateProxy(
      ValueStateDescriptor<T> stateDescriptor) {
    return new ValueStateStoreManagerProxy<>(this, stateDescriptor);
  }

  public <T> ValueState<T> getValueState(ValueStateDescriptor<T> stateDescriptor) {
    String desc = stateDescriptor.getIdentify();
    if (valueManagerProxyHashMap.containsKey(desc)) {
      return valueManagerProxyHashMap.get(desc).getValueState();
    } else {
      ValueStateStoreManagerProxy<T> valueStateProxy = newValueStateProxy(stateDescriptor);
      valueManagerProxyHashMap.put(desc, valueStateProxy);
      return valueStateProxy.getValueState();
    }
  }

  /**
   * list State
   */
  protected <T> ListStateStoreManagerProxy<T> newListStateProxy(
      ListStateDescriptor<T> stateDescriptor) {
    return new ListStateStoreManagerProxy<>(this, stateDescriptor);
  }

  public <T> ListState<T> getListState(ListStateDescriptor<T> stateDescriptor) {
    String desc = stateDescriptor.getIdentify();
    if (listManagerProxyHashMap.containsKey(desc)) {
      ListStateStoreManagerProxy<T> listStateProxy = listManagerProxyHashMap.get(desc);
      return listStateProxy.getListState();
    } else {
      ListStateStoreManagerProxy<T> listStateProxy = newListStateProxy(stateDescriptor);
      listManagerProxyHashMap.put(desc, listStateProxy);
      return listStateProxy.getListState();
    }
  }

  /**
   * map state
   */
  protected <S, T> MapStateStoreManagerProxy<S, T> newMapStateProxy(
      MapStateDescriptor<S, T> stateDescriptor) {
    return new MapStateStoreManagerProxy<>(this, stateDescriptor);
  }

  public <S, T> MapState<S, T> getMapState(MapStateDescriptor<S, T> stateDescriptor) {
    String desc = stateDescriptor.getIdentify();
    if (mapManagerProxyHashMap.containsKey(desc)) {
      MapStateStoreManagerProxy<S, T> mapStateProxy = mapManagerProxyHashMap.get(desc);
      return mapStateProxy.getMapState();
    } else {
      MapStateStoreManagerProxy<S, T> mapStateProxy = newMapStateProxy(stateDescriptor);
      mapManagerProxyHashMap.put(desc, mapStateProxy);
      return mapStateProxy.getMapState();
    }
  }

  @Override
  public void setCurrentKey(Object currentKey) {
    super.keyGroupIndex = KeyGroupAssignment.assignKeyGroupIndexForKey(
      currentKey, numberOfKeyGroups);
    super.currentKey = currentKey;
  }

  public int getNumberOfKeyGroups() {
    return numberOfKeyGroups;
  }

  public KeyGroup getKeyGroup() {
    return keyGroup;
  }

  public void close() {
    for (ValueStateStoreManagerProxy proxy : valueManagerProxyHashMap.values()) {
      proxy.close();
    }
    for (ListStateStoreManagerProxy proxy : listManagerProxyHashMap.values()) {
      proxy.close();
    }
    for (MapStateStoreManagerProxy proxy : mapManagerProxyHashMap.values()) {
      proxy.close();
    }
  }
}
