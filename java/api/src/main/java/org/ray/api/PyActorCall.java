// Generated by `RayCallGenerator.java`. DO NOT EDIT.

package org.ray.api;

/**
 * This class provides type-safe interfaces for remote actor calls.
 **/
@SuppressWarnings({"rawtypes", "unchecked"})
interface PyActorCall<A> {

  default RayObject call(String functionName) {
    Object[] args = new Object[]{};
    return Ray.internal().callPyActor((RayPyActor)this, functionName, args);
  }

  default RayObject call(String functionName, Object obj0) {
    Object[] args = new Object[]{obj0};
    return Ray.internal().callPyActor((RayPyActor)this, functionName, args);
  }

  default RayObject call(String functionName, Object obj0, Object obj1) {
    Object[] args = new Object[]{obj0, obj1};
    return Ray.internal().callPyActor((RayPyActor)this, functionName, args);
  }

  default RayObject call(String functionName, Object obj0, Object obj1, Object obj2) {
    Object[] args = new Object[]{obj0, obj1, obj2};
    return Ray.internal().callPyActor((RayPyActor)this, functionName, args);
  }

  default RayObject call(String functionName, Object obj0, Object obj1, Object obj2, Object obj3) {
    Object[] args = new Object[]{obj0, obj1, obj2, obj3};
    return Ray.internal().callPyActor((RayPyActor)this, functionName, args);
  }

  default RayObject call(String functionName, Object obj0, Object obj1, Object obj2, Object obj3, Object obj4) {
    Object[] args = new Object[]{obj0, obj1, obj2, obj3, obj4};
    return Ray.internal().callPyActor((RayPyActor)this, functionName, args);
  }

}
