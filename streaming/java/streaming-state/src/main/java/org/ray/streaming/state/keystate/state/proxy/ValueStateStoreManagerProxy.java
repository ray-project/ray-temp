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

package org.ray.streaming.state.keystate.state.proxy;

import org.ray.streaming.state.IKVState;
import org.ray.streaming.state.backend.KeyStateBackend;
import org.ray.streaming.state.keystate.desc.ValueStateDescriptor;
import org.ray.streaming.state.keystate.state.ValueState;
import org.ray.streaming.state.keystate.state.impl.ValueStateImpl;
import org.ray.streaming.state.strategy.TransactionStateStoreManagerProxy;

/**
 * This class defines ValueState Wrapper, connecting state and backend.
 */
public class ValueStateStoreManagerProxy<T> extends TransactionStateStoreManagerProxy<T> implements
    IKVState<String, T> {

  private final ValueStateImpl<T> valueState;

  public ValueStateStoreManagerProxy(KeyStateBackend keyStateBackend,
                                     ValueStateDescriptor<T> stateDescriptor) {
    super(keyStateBackend, stateDescriptor);
    this.valueState = new ValueStateImpl<>(stateDescriptor, keyStateBackend);
  }

  public ValueState<T> getValueState() {
    return this.valueState;
  }
}
