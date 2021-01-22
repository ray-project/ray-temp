package io.ray.api.placementgroup;

import io.ray.api.id.PlacementGroupId;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A placement group is used to place interdependent actors according to a specific strategy {@link
 * PlacementStrategy}. When a placement group is created, the corresponding actor slots and
 * resources are preallocated. A placement group consists of one or more bundles plus a specific
 * placement strategy.
 */
public interface PlacementGroup {

  /**
   * Get the id of current placement group.
   * @return Id of current placement group.
   */
  PlacementGroupId getId();

  /**
   * Get the name of current placement group.
   * @return Name of current placement group.
   */
  String getName();

  /**
   * Get all bundles of current placement group.
   * @return All bundles of current placement group.
   */
  List<Map<String, Double>> getBundles();

  /**
   * Get the strategy of current placement group.
   * @return Strategy of current placement group.
   */
  PlacementStrategy getStrategy();

  /**
   * Get the state of current placement group.
   * @return Creation state of current placement group.
   */
  PlacementGroupState getState();

  /**
   * Wait for the placement group to be ready within the specified time.
   *
   * @param duration the time duration in the given.
   * @param unit the unit of the {@code duration} argument.
   * @return True if the placement group is created. False otherwise.
   */
  boolean wait(long duration, TimeUnit unit);
}
