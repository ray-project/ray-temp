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

import org.ray.streaming.state.keystate.desc.ListStateDescriptor;
import org.ray.streaming.state.keystate.state.ListState;
import org.ray.streaming.state.keystate.state.impl.OperatorStateImpl;
import org.ray.streaming.state.keystate.state.proxy.ListStateStoreManagerProxy;

/**
 * OperatorState manager.
 */
public class OperatorStateBackend extends TransactionKeyStateBackend {

  public OperatorStateBackend(AbstractStateBackend backend) {
    super(backend);
  }

  @Override
  public void setCurrentKey(Object currentKey) {
    super.currentKey = currentKey;
  }

  protected <T> ListStateStoreManagerProxy<T> newListStateStoreManagerProxy(
      ListStateDescriptor<T> stateDescriptor) {
    return new ListStateStoreManagerProxy<>(this, stateDescriptor);
  }

  /**
   * splitList
   */
  public <T> ListState<T> getSplitListState(ListStateDescriptor<T> stateDescriptor) {
    String desc = stateDescriptor.getIdentify();
    if (listManagerProxyHashMap.containsKey(desc)) {
      ListStateStoreManagerProxy<T> listStateFacade = listManagerProxyHashMap.get(desc);
      return listStateFacade.getListState();
    } else {
      ListStateStoreManagerProxy<T> listStateFacade = newListStateStoreManagerProxy(
          stateDescriptor);
      listManagerProxyHashMap.put(desc, listStateFacade);
      ((OperatorStateImpl) (listStateFacade.getListState())).setSplit(true);
      return listStateFacade.getListState();
    }
  }

  /**
   * unionList
   */
  public <T> ListState<T> getUnionListState(ListStateDescriptor<T> stateDescriptor) {
    String desc = stateDescriptor.getIdentify();
    if (listManagerProxyHashMap.containsKey(desc)) {
      ListStateStoreManagerProxy<T> listStateFacade = listManagerProxyHashMap.get(desc);
      return listStateFacade.getListState();
    } else {
      ListStateStoreManagerProxy<T> listStateFacade = newListStateStoreManagerProxy(
          stateDescriptor);
      listManagerProxyHashMap.put(desc, listStateFacade);
      ((OperatorStateImpl) (listStateFacade.getListState())).init();
      return listStateFacade.getListState();
    }
  }
}
