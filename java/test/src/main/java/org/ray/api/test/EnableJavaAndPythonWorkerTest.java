package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.annotation.RayRemote;
import org.ray.runtime.util.FileUtil;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EnableJavaAndPythonWorkerTest {

  public static final String PLASMA_STORE_SOCKET_NAME = "/tmp/ray/test/plasma_store_socket";
  public static final String RAYLET_SOCKET_NAME = "/tmp/ray/test/raylet_socket";

  @RayRemote
  public static String echo(String word) {
    return word;
  }

  @BeforeMethod
  public void setUp() {
    // TODO(qwang): This is a workaround approach.
    // Since `ray start --raylet-socket-name=tmp/xxx/yyy/raylet_socket` could
    // not create the folders if they don't exist, we should make sure that
    // `/tmp/xxx/yyy` must exist. After we fix this issue, these codes could be
    // removed.
    File baseDir = new File("/tmp/ray/test");
    FileUtil.mkDir(baseDir);
  }

  @AfterMethod
  public void tearDown() {
    //clean some files.
    File plasmaSocket = new File(PLASMA_STORE_SOCKET_NAME);
    if (plasmaSocket.exists()) {
      plasmaSocket.delete();
    }
  }

  @Test
  public void testEnableJavaAndPythonWorker() throws IOException, InterruptedException {

    final String rayCommand = getRayCommand();
    if (rayCommand == null) {
      throw new SkipException("Since there is no ray command, we skip this test.");
    }

    startCluster(rayCommand);

    System.setProperty("ray.home", "../..");
    System.setProperty("ray.redis.address", "127.0.0.1:6379");
    System.setProperty("ray.object-store.socket-name", PLASMA_STORE_SOCKET_NAME);
    System.setProperty("ray.raylet.socket-name", RAYLET_SOCKET_NAME);
    Ray.init();

    RayObject<String> obj = Ray.call(EnableJavaAndPythonWorkerTest::echo, "hello");
    Assert.assertEquals("hello", obj.get());

    Ray.shutdown();
    System.clearProperty("ray.home");
    System.clearProperty("ray.redis.address");
    System.clearProperty("ray.object-store.socket-name");
    System.clearProperty("ray.raylet.socket-name");

    stopCluster(rayCommand);
  }

  private void startCluster(String rayCommand) throws IOException, InterruptedException {
    final List<String> startCommand = ImmutableList.of(
        rayCommand,
        "start",
        "--head",
        "--redis-port=6379",
        "--include-java",
        String.format("--plasma-store-socket-name=%s", PLASMA_STORE_SOCKET_NAME),
        String.format("--raylet-socket-name=%s", RAYLET_SOCKET_NAME),
        "--java-worker-options=-classpath ../../build/java/*:../../java/test/target/*"
    );

    ProcessBuilder builder = new ProcessBuilder(startCommand);
    Process process = builder.start();
    process.waitFor(500, TimeUnit.MILLISECONDS);
  }

  private void stopCluster(String rayCommand) throws IOException, InterruptedException {
    final List<String> stopCommand = ImmutableList.of(
        rayCommand,
        "stop"
    );

    ProcessBuilder builder = new ProcessBuilder(stopCommand);
    Process process = builder.start();
    process.waitFor(500, TimeUnit.MILLISECONDS);

    // make sure `ray stop` command get finished. Otherwise some kill command
    // will terminate the processes which is created by next test case.
    TimeUnit.SECONDS.sleep(1);
  }

  private String getRayCommand() throws IOException, InterruptedException {
    Process process = Runtime.getRuntime().exec("which python");
    process.waitFor(500, TimeUnit.MILLISECONDS);

    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
    String pythonPath = reader.readLine();
    if (!pythonPath.endsWith("/python")) {
      return null;
    }

    String binPath = pythonPath.substring(0, pythonPath.length() - "/python".length());
    String rayPath = String.format("%s/ray", binPath);
    try {
      process = Runtime.getRuntime().exec(rayPath);
    } catch (IOException e) {
      // IOException means that no such file or directory.
      return null;
    }
    process.waitFor(1000, TimeUnit.MILLISECONDS);

    reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
    String result = reader.readLine();
    if (result.startsWith("Usage:")) {
      return rayPath;
    }

    return null;
  }

}
