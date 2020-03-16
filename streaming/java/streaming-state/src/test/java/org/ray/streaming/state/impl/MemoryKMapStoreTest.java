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

package org.ray.streaming.state.impl;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.ray.streaming.state.backend.AbstractStateBackend;
import org.ray.streaming.state.backend.StateBackendBuilder;
import org.ray.streaming.state.store.IKMapStore;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class MemoryKMapStoreTest {

  private AbstractStateBackend stateBackend;
  private IKMapStore<String, String, String> ikMapStore;

  @BeforeClass
  public void setUp() {
    stateBackend = StateBackendBuilder.buildStateBackend(new HashMap<String, String>());
    ikMapStore = stateBackend.getKeyMapStore("test-table");
  }

  @Test
  public void testCase() {
    try {
      Assert.assertNull(ikMapStore.get("hello"));
      Map<String, String> map = Maps.newHashMap();
      map.put("1", "1-1");
      map.put("2", "2-1");

      ikMapStore.put("hello", map);
      Assert.assertEquals(ikMapStore.get("hello"), map);

      Map<String, String> map2 = Maps.newHashMap();
      map.put("3", "3-1");
      map.put("4", "4-1");
      ikMapStore.put("hello", map2);
      Assert.assertNotEquals(ikMapStore.get("hello"), map);
      Assert.assertEquals(ikMapStore.get("hello"), map2);


    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
