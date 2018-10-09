package org.ray.api.runtime;

/**
 * A factory that produces a RayRuntime instance.
 */
public interface RayRuntimeFactory {

  RayRuntime createRayRuntime(String config);
}
