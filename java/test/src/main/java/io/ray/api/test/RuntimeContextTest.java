package io.ray.api.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.id.ActorId;
import io.ray.api.id.JobId;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class RuntimeContextTest extends BaseTest {

  private static JobId JOB_ID = getJobId();
  private static String RAYLET_SOCKET_NAME = "/tmp/ray/test/raylet_socket";
  private static String OBJECT_STORE_SOCKET_NAME = "/tmp/ray/test/object_store_socket";

  private static JobId getJobId() {
    // Must be stable across different processes.
    byte[] bytes = new byte[JobId.LENGTH];
    Arrays.fill(bytes, (byte) 127);
    return JobId.fromByteBuffer(ByteBuffer.wrap(bytes));
  }

  @BeforeClass
  public void setUp() {
    System.setProperty("ray.job.id", JOB_ID.toString());
    System.setProperty("ray.raylet.socket-name", RAYLET_SOCKET_NAME);
    System.setProperty("ray.object-store.socket-name", OBJECT_STORE_SOCKET_NAME);
  }

  @AfterClass
  public void tearDown() {
    System.clearProperty("ray.job.id");
    System.clearProperty("ray.raylet.socket-name");
    System.clearProperty("ray.object-store.socket-name");
  }

  @Test
  public void testRuntimeContextInDriver() {
    Assert.assertEquals(JOB_ID, Ray.getRuntimeContext().getCurrentJobId());
    Assert.assertEquals(RAYLET_SOCKET_NAME, Ray.getRuntimeContext().getRayletSocketName());
    Assert.assertEquals(OBJECT_STORE_SOCKET_NAME,
        Ray.getRuntimeContext().getObjectStoreSocketName());
  }

  public static class RuntimeContextTester {

    public String testRuntimeContext(ActorId actorId) {
      Assert.assertEquals(JOB_ID, Ray.getRuntimeContext().getCurrentJobId());
      Assert.assertEquals(actorId, Ray.getRuntimeContext().getCurrentActorId());
      Assert.assertEquals(RAYLET_SOCKET_NAME, Ray.getRuntimeContext().getRayletSocketName());
      Assert.assertEquals(OBJECT_STORE_SOCKET_NAME,
          Ray.getRuntimeContext().getObjectStoreSocketName());
      return "ok";
    }
  }

  @Test
  public void testRuntimeContextInActor() {
    ActorHandle<RuntimeContextTester> actor = Ray.createActor(RuntimeContextTester::new);
    Assert.assertEquals("ok",
        actor.call(RuntimeContextTester::testRuntimeContext, actor.getId()).get());
  }

}
