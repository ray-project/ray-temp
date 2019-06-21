package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.TestUtils;
import org.ray.api.annotation.RayRemote;
import org.ray.api.id.ObjectId;
import org.ray.runtime.AbstractRayRuntime;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PlasmaFreeTest extends BaseTest {

  @RayRemote
  private static String hello() {
    return "hello";
  }

  @Test
  public void testDeleteObjects() {
    RayObject<String> helloId = Ray.call(PlasmaFreeTest::hello);
    String helloString = helloId.get();
    Assert.assertEquals("hello", helloString);
    Ray.internal().free(ImmutableList.of(helloId.getId()), true, false);

    List<ObjectId> ids = new ArrayList<>();
    ids.add(helloId.getId());
    final boolean result = TestUtils.waitForCondition(() -> !((AbstractRayRuntime) Ray.internal())
        .getWorker().getObjectInterface().get(ids, 0).get(0).exists, 50);
    Assert.assertTrue(result);
  }

  @Test
  public void testDeleteCreatingTasks() {
    TestUtils.skipTestUnderSingleProcess();
    RayObject<String> helloId = Ray.call(PlasmaFreeTest::hello);
    Assert.assertEquals("hello", helloId.get());
    Ray.internal().free(ImmutableList.of(helloId.getId()), true, true);

    final boolean result = TestUtils.waitForCondition(
        () ->  !(((AbstractRayRuntime)Ray.internal()).getGcsClient())
          .rayletTaskExistsInGcs(helloId.getId().getTaskId()), 50);
    Assert.assertTrue(result);
  }

}
