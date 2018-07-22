package org.ray.exercise;

import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.RayRemote;
import org.ray.core.RayRuntime;

/**
 * Call remote functions from within remote functions.
 */
public class Exercise03 {

  /**
   * Remote function which will call another remote function.
   */
  @RayRemote
  public static String sayHelloWithWorld() {
    String ret = "hello";
    System.out.println(ret);
    RayObject<String> world = Ray.call(Exercise03::sayWorld);
    return ret + "," + world.get();
  }

  /**
   * Remote function which will be called by another remote function.
   */
  @RayRemote
  public static String sayWorld() {
    String ret = "world!";
    System.out.println(ret);
    return ret;
  }

  /**
   * Main.
   */
  public static void main(String[] args) throws Exception {
    try {
      Ray.init();
      String helloWithWorld = Ray.call(Exercise03::sayHelloWithWorld).get();
      System.out.println(helloWithWorld);
    } catch (Throwable t) {
      t.printStackTrace();
    } finally {
      RayRuntime.getInstance().cleanUp();
    }
  }
}
