package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import org.testng.Assert;
import org.testng.annotations.Test;

public class WorkerJvmOptionsTest extends BaseTest {

  public static class Echo {
    String getOptions() {
      return System.getProperty("test.suffix");
    }
  }

  @Test
  public void testJvmOptions() {
    TestUtils.skipTestUnderSingleProcess();
    // The whitespaces in following argument are intentionally added to test
    // that raylet can correctly handle dynamic options with whitespaces.
    ActorHandle<Echo> actor = Ray.actor(Echo::new)
        .setJvmOptions(" -Dtest.suffix=suffix -Dtest.suffix1=suffix1 ")
        .remote();
    ObjectRef<String> obj = actor.task(Echo::getOptions).remote();
    Assert.assertEquals(obj.get(), "suffix");
  }
}
