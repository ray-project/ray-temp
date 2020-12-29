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

package io.ray.streaming.state.keystate.desc;

import org.testng.Assert;
import org.testng.annotations.Test;

public class MapStateDescriptorTest {

  @Test
  public void test() {
    MapStateDescriptor<String, Integer> descriptor =
        MapStateDescriptor.build("msdTest", String.class, Integer.class);

    descriptor.setTableName("table");
    Assert.assertEquals(descriptor.getTableName(), "table");
    Assert.assertEquals(descriptor.getName(), "msdTest");
  }
}
