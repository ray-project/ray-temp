package org.ray.api.benchmark;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.RayRemote;
import org.ray.api.test.MyRunner;

@RunWith(MyRunner.class)
public class MaxPressureTest extends RayBenchmarkTest {

  private static final long serialVersionUID = -1684518885171395952L;

  public static final int clientNum = 2;

  public static final int totalNum = 10;

  @Test
  public void Test() {
    PressureTestParameter pressureTestParameter = new PressureTestParameter();
    pressureTestParameter.setClientNum(clientNum);
    pressureTestParameter.setTotalNum(totalNum);
    pressureTestParameter.setRayBenchmarkTest(this);
    super.maxPressureTest(pressureTestParameter);
  }

  @RayRemote
  public static RemoteResult<Integer> currentTime() {
    RemoteResult<Integer> remoteResult = new RemoteResult<>();
    remoteResult.setFinishTime(System.nanoTime());
    remoteResult.setResult(0);
    return remoteResult;
  }

  @Override
  public boolean checkResult(Object o) {
    return (int) o == 0;
  }

  @Override
  public RayObject<RemoteResult<Integer>> rayCall(RayActor rayActor) {

    return Ray.call(MaxPressureTest::currentTime);
  }

}
