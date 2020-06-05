package io.ray.api;

/**
 * Handle of a Python actor.
 */
public interface RayPyActor extends BaseActorHandle, PyActorCall {

  /**
   * @return Module name of the Python actor class.
   */
  String getModuleName();

  /**
   * @return Name of the Python actor class.
   */
  String getClassName();
}

