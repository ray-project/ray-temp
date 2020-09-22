package io.ray.api.options;

import java.util.HashMap;
import java.util.Map;

/**
 * The options class for RayCall or ActorCreation.
 */
public abstract class BaseTaskOptions {

  public final Map<String, Double> resources;

  public BaseTaskOptions() {
    resources = new HashMap<>();
  }

  public BaseTaskOptions(Map<String, Double> resources) {
    for (Map.Entry<String, Double> entry : resources.entrySet()) {
      if (entry.getValue() == null || entry.getValue().compareTo(0.0) <= 0) {
        throw new IllegalArgumentException(String.format("Resource capacity should be "
            + "positive, but got resource %s = %s.", entry.getKey(), entry.getValue()));
      }
      // Note: resource value should be an integer if it is greater than 1.0, like 3.0 is legal,
      // but 3.5 is illegal.
      if (entry.getValue().compareTo(1.0) >= 0
          && entry.getValue().compareTo(Math.floor(entry.getValue())) != 0) {
        throw new IllegalArgumentException(String.format("Resource capacity should be "
                + "a positive integer if it is greater than 1.0, but got resource %s = %s.",
            entry.getKey(), entry.getValue()));
      }
    }
    this.resources = resources;
  }

}
