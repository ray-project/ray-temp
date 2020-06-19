package io.ray.api.options;

import io.ray.api.Ray;
import java.util.HashMap;
import java.util.Map;

/**
 * The options for creating actor.
 */
public class ActorCreationOptions extends BaseTaskOptions {
  public final String name;
  public final int maxRestarts;
  public final String jvmOptions;
  public final int maxConcurrency;

  private ActorCreationOptions(String name, Map<String, Double> resources, int maxRestarts,
                               String jvmOptions, int maxConcurrency) {
    super(resources);
    this.name = name;
    this.maxRestarts = maxRestarts;
    this.jvmOptions = jvmOptions;
    this.maxConcurrency = maxConcurrency;
  }

  /**
   * The inner class for building ActorCreationOptions.
   */
  public static class Builder {
    private String name;
    private Map<String, Double> resources = new HashMap<>();
    private int maxRestarts = 0;
    private String jvmOptions = null;
    private int maxConcurrency = 1;

    /**
     * Set the actor name of a named actor.
     *
     * @param name The name of the named actor.
     * @param global Whether this actor is a global actor
     * @return self
     */
    public Builder setName(String name, boolean global) {
      if (global) {
        this.name = String.format("%s-%s", Ray.getRuntimeContext().getCurrentJobId(), name);
      } else {
        this.name = name;
      }
      return this;
    }

    /**
     * Set a custom resource requirement to reserve for the lifetime of this actor.
     * This method can be called multiple times. If the same resource is set multiple times,
     * the latest quantity will be used.
     *
     * @param resourceName resource name
     * @param resourceQuantity resource quantity
     * @return self
     */
    public Builder setResource(String resourceName, Double resourceQuantity) {
      this.resources.put(resourceName, resourceQuantity);
      return this;
    }

    /**
     * Set custom resource requirements to reserve for the lifetime of this actor.
     * This method can be called multiple times. If the same resource is set multiple times,
     * the latest quantity will be used.
     *
     * @param resources requirements for multiple resources.
     * @return self
     */
    public Builder setResources(Map<String, Double> resources) {
      this.resources.putAll(resources);
      return this;
    }

    /**
     * This specifies the maximum number of times that the actor should be restarted when it dies
     * unexpectedly. The minimum valid value is 0 (default), which indicates that the actor doesn't
     * need to be restarted. A value of -1 indicates that an actor should be restarted indefinitely.
     *
     * @param maxRestarts max number of actor restarts
     * @return self
     */
    public Builder setMaxRestarts(int maxRestarts) {
      this.maxRestarts = maxRestarts;
      return this;
    }

    /**
     * Set the JVM options for the Java worker that this actor is running in.
     * <p>
     * Note, if this is set, this actor won't share Java worker with other actors or tasks.
     *
     * @param jvmOptions JVM options for the Java worker that this actor is running in.
     * @return self
     */
    public Builder setJvmOptions(String jvmOptions) {
      this.jvmOptions = jvmOptions;
      return this;
    }

    /**
     * Set the max number of concurrent calls to allow for this actor.
     * <p>
     * The max concurrency defaults to 1 for threaded execution.
     * Note that the execution order is not guaranteed when max_concurrency > 1.
     *
     * @param maxConcurrency The max number of concurrent calls to allow for this actor.
     * @return self
     */
    public Builder setMaxConcurrency(int maxConcurrency) {
      if (maxConcurrency <= 0) {
        throw new IllegalArgumentException("maxConcurrency must be greater than 0.");
      }

      this.maxConcurrency = maxConcurrency;
      return this;
    }

    public ActorCreationOptions build() {
      return new ActorCreationOptions(
          name, resources, maxRestarts, jvmOptions, maxConcurrency);
    }
  }

}
