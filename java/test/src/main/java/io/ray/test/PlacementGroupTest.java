package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.id.ActorId;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.runtime.placementgroup.PlacementGroupImpl;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class PlacementGroupTest extends BaseTest {

  public static class Counter {

    private int value;

    public Counter(int initValue) {
      this.value = initValue;
    }

    public int getValue() {
      return value;
    }
  }

  // TODO(ffbin): Currently Java doesn't support multi-node tests.
  // This test just creates a placement group with one bundle.
  // It's not comprehensive to test all placement group test cases.
  public void testCreateAndCallActor() {
    PlacementGroup placementGroup = PlacementGroupTestUtils.createSimpleGroup();
    Assert.assertEquals(((PlacementGroupImpl)placementGroup).getName(),"unnamed_group");

    // Test creating an actor from a constructor.
    ActorHandle<Counter> actor = Ray.actor(Counter::new, 1)
        .setPlacementGroup(placementGroup, 0).remote();
    Assert.assertNotEquals(actor.getId(), ActorId.NIL);

    // Test calling an actor.
    Assert.assertEquals(Integer.valueOf(1), actor.task(Counter::getValue).remote().get());
  }

  public void testCheckBundleIndex() {
    PlacementGroup placementGroup = PlacementGroupTestUtils.createSimpleGroup();

    int exceptionCount = 0;
    try {
      Ray.actor(Counter::new, 1).setPlacementGroup(placementGroup, 1).remote();
    } catch (IllegalArgumentException e) {
      ++exceptionCount;
    }
    Assert.assertEquals(1, exceptionCount);

    try {
      Ray.actor(Counter::new, 1).setPlacementGroup(placementGroup, -1).remote();
    } catch (IllegalArgumentException e) {
      ++exceptionCount;
    }
    Assert.assertEquals(2, exceptionCount);
  }

  @Test (expectedExceptions = { IllegalArgumentException.class })
  public void testBundleSizeValidCheckWhenCreate() {
    PlacementGroupTestUtils.createBundleSizeInvalidGroup();
  }

  @Test (expectedExceptions = { IllegalArgumentException.class })
  public void testBundleResourceValidCheckWhenCreate() {
    PlacementGroupTestUtils.createBundleResourceInvalidGroup();
  }
}
