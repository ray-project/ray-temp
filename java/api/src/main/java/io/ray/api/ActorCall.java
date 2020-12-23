// Generated by `RayCallGenerator.java`. DO NOT EDIT.

package io.ray.api;

import io.ray.api.call.ActorTaskCaller;
import io.ray.api.call.VoidActorTaskCaller;
import io.ray.api.function.RayFunc1;
import io.ray.api.function.RayFunc2;
import io.ray.api.function.RayFunc3;
import io.ray.api.function.RayFunc4;
import io.ray.api.function.RayFunc5;
import io.ray.api.function.RayFunc6;
import io.ray.api.function.RayFuncVoid1;
import io.ray.api.function.RayFuncVoid2;
import io.ray.api.function.RayFuncVoid3;
import io.ray.api.function.RayFuncVoid4;
import io.ray.api.function.RayFuncVoid5;
import io.ray.api.function.RayFuncVoid6;

/**
 * This class provides type-safe interfaces for remote actor calls.
 **/
interface ActorCall<A> {

  default <R> ActorTaskCaller<R> task(RayFunc1<A, R> f) {
    Object[] args = new Object[] {};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default VoidActorTaskCaller task(RayFuncVoid1<A> f) {
    Object[] args = new Object[] {};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, R> ActorTaskCaller<R> task(RayFunc2<A, T0, R> f, T0 t0) {
    Object[] args = new Object[] {t0};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, R> ActorTaskCaller<R> task(RayFunc2<A, T0, R> f, ObjectRef<T0> t0) {
    Object[] args = new Object[] {t0};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0> VoidActorTaskCaller task(RayFuncVoid2<A, T0> f, T0 t0) {
    Object[] args = new Object[] {t0};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0> VoidActorTaskCaller task(RayFuncVoid2<A, T0> f, ObjectRef<T0> t0) {
    Object[] args = new Object[] {t0};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, R> ActorTaskCaller<R> task(RayFunc3<A, T0, T1, R> f, T0 t0, T1 t1) {
    Object[] args = new Object[] {t0, t1};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, R> ActorTaskCaller<R> task(RayFunc3<A, T0, T1, R> f, T0 t0, ObjectRef<T1> t1) {
    Object[] args = new Object[] {t0, t1};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, R> ActorTaskCaller<R> task(RayFunc3<A, T0, T1, R> f, ObjectRef<T0> t0, T1 t1) {
    Object[] args = new Object[] {t0, t1};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, R> ActorTaskCaller<R> task(RayFunc3<A, T0, T1, R> f, ObjectRef<T0> t0, ObjectRef<T1> t1) {
    Object[] args = new Object[] {t0, t1};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1> VoidActorTaskCaller task(RayFuncVoid3<A, T0, T1> f, T0 t0, T1 t1) {
    Object[] args = new Object[] {t0, t1};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1> VoidActorTaskCaller task(RayFuncVoid3<A, T0, T1> f, T0 t0, ObjectRef<T1> t1) {
    Object[] args = new Object[] {t0, t1};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1> VoidActorTaskCaller task(RayFuncVoid3<A, T0, T1> f, ObjectRef<T0> t0, T1 t1) {
    Object[] args = new Object[] {t0, t1};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1> VoidActorTaskCaller task(RayFuncVoid3<A, T0, T1> f, ObjectRef<T0> t0, ObjectRef<T1> t1) {
    Object[] args = new Object[] {t0, t1};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, R> ActorTaskCaller<R> task(RayFunc4<A, T0, T1, T2, R> f, T0 t0, T1 t1, T2 t2) {
    Object[] args = new Object[] {t0, t1, t2};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, R> ActorTaskCaller<R> task(RayFunc4<A, T0, T1, T2, R> f, T0 t0, T1 t1, ObjectRef<T2> t2) {
    Object[] args = new Object[] {t0, t1, t2};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, R> ActorTaskCaller<R> task(RayFunc4<A, T0, T1, T2, R> f, T0 t0, ObjectRef<T1> t1, T2 t2) {
    Object[] args = new Object[] {t0, t1, t2};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, R> ActorTaskCaller<R> task(RayFunc4<A, T0, T1, T2, R> f, T0 t0, ObjectRef<T1> t1, ObjectRef<T2> t2) {
    Object[] args = new Object[] {t0, t1, t2};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, R> ActorTaskCaller<R> task(RayFunc4<A, T0, T1, T2, R> f, ObjectRef<T0> t0, T1 t1, T2 t2) {
    Object[] args = new Object[] {t0, t1, t2};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, R> ActorTaskCaller<R> task(RayFunc4<A, T0, T1, T2, R> f, ObjectRef<T0> t0, T1 t1, ObjectRef<T2> t2) {
    Object[] args = new Object[] {t0, t1, t2};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, R> ActorTaskCaller<R> task(RayFunc4<A, T0, T1, T2, R> f, ObjectRef<T0> t0, ObjectRef<T1> t1, T2 t2) {
    Object[] args = new Object[] {t0, t1, t2};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, R> ActorTaskCaller<R> task(RayFunc4<A, T0, T1, T2, R> f, ObjectRef<T0> t0, ObjectRef<T1> t1, ObjectRef<T2> t2) {
    Object[] args = new Object[] {t0, t1, t2};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2> VoidActorTaskCaller task(RayFuncVoid4<A, T0, T1, T2> f, T0 t0, T1 t1, T2 t2) {
    Object[] args = new Object[] {t0, t1, t2};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2> VoidActorTaskCaller task(RayFuncVoid4<A, T0, T1, T2> f, T0 t0, T1 t1, ObjectRef<T2> t2) {
    Object[] args = new Object[] {t0, t1, t2};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2> VoidActorTaskCaller task(RayFuncVoid4<A, T0, T1, T2> f, T0 t0, ObjectRef<T1> t1, T2 t2) {
    Object[] args = new Object[] {t0, t1, t2};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2> VoidActorTaskCaller task(RayFuncVoid4<A, T0, T1, T2> f, T0 t0, ObjectRef<T1> t1, ObjectRef<T2> t2) {
    Object[] args = new Object[] {t0, t1, t2};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2> VoidActorTaskCaller task(RayFuncVoid4<A, T0, T1, T2> f, ObjectRef<T0> t0, T1 t1, T2 t2) {
    Object[] args = new Object[] {t0, t1, t2};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2> VoidActorTaskCaller task(RayFuncVoid4<A, T0, T1, T2> f, ObjectRef<T0> t0, T1 t1, ObjectRef<T2> t2) {
    Object[] args = new Object[] {t0, t1, t2};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2> VoidActorTaskCaller task(RayFuncVoid4<A, T0, T1, T2> f, ObjectRef<T0> t0, ObjectRef<T1> t1, T2 t2) {
    Object[] args = new Object[] {t0, t1, t2};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2> VoidActorTaskCaller task(RayFuncVoid4<A, T0, T1, T2> f, ObjectRef<T0> t0, ObjectRef<T1> t1, ObjectRef<T2> t2) {
    Object[] args = new Object[] {t0, t1, t2};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, R> ActorTaskCaller<R> task(RayFunc5<A, T0, T1, T2, T3, R> f, T0 t0, T1 t1, T2 t2, T3 t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, R> ActorTaskCaller<R> task(RayFunc5<A, T0, T1, T2, T3, R> f, T0 t0, T1 t1, T2 t2, ObjectRef<T3> t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, R> ActorTaskCaller<R> task(RayFunc5<A, T0, T1, T2, T3, R> f, T0 t0, T1 t1, ObjectRef<T2> t2, T3 t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, R> ActorTaskCaller<R> task(RayFunc5<A, T0, T1, T2, T3, R> f, T0 t0, T1 t1, ObjectRef<T2> t2, ObjectRef<T3> t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, R> ActorTaskCaller<R> task(RayFunc5<A, T0, T1, T2, T3, R> f, T0 t0, ObjectRef<T1> t1, T2 t2, T3 t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, R> ActorTaskCaller<R> task(RayFunc5<A, T0, T1, T2, T3, R> f, T0 t0, ObjectRef<T1> t1, T2 t2, ObjectRef<T3> t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, R> ActorTaskCaller<R> task(RayFunc5<A, T0, T1, T2, T3, R> f, T0 t0, ObjectRef<T1> t1, ObjectRef<T2> t2, T3 t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, R> ActorTaskCaller<R> task(RayFunc5<A, T0, T1, T2, T3, R> f, T0 t0, ObjectRef<T1> t1, ObjectRef<T2> t2, ObjectRef<T3> t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, R> ActorTaskCaller<R> task(RayFunc5<A, T0, T1, T2, T3, R> f, ObjectRef<T0> t0, T1 t1, T2 t2, T3 t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, R> ActorTaskCaller<R> task(RayFunc5<A, T0, T1, T2, T3, R> f, ObjectRef<T0> t0, T1 t1, T2 t2, ObjectRef<T3> t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, R> ActorTaskCaller<R> task(RayFunc5<A, T0, T1, T2, T3, R> f, ObjectRef<T0> t0, T1 t1, ObjectRef<T2> t2, T3 t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, R> ActorTaskCaller<R> task(RayFunc5<A, T0, T1, T2, T3, R> f, ObjectRef<T0> t0, T1 t1, ObjectRef<T2> t2, ObjectRef<T3> t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, R> ActorTaskCaller<R> task(RayFunc5<A, T0, T1, T2, T3, R> f, ObjectRef<T0> t0, ObjectRef<T1> t1, T2 t2, T3 t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, R> ActorTaskCaller<R> task(RayFunc5<A, T0, T1, T2, T3, R> f, ObjectRef<T0> t0, ObjectRef<T1> t1, T2 t2, ObjectRef<T3> t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, R> ActorTaskCaller<R> task(RayFunc5<A, T0, T1, T2, T3, R> f, ObjectRef<T0> t0, ObjectRef<T1> t1, ObjectRef<T2> t2, T3 t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, R> ActorTaskCaller<R> task(RayFunc5<A, T0, T1, T2, T3, R> f, ObjectRef<T0> t0, ObjectRef<T1> t1, ObjectRef<T2> t2, ObjectRef<T3> t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3> VoidActorTaskCaller task(RayFuncVoid5<A, T0, T1, T2, T3> f, T0 t0, T1 t1, T2 t2, T3 t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3> VoidActorTaskCaller task(RayFuncVoid5<A, T0, T1, T2, T3> f, T0 t0, T1 t1, T2 t2, ObjectRef<T3> t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3> VoidActorTaskCaller task(RayFuncVoid5<A, T0, T1, T2, T3> f, T0 t0, T1 t1, ObjectRef<T2> t2, T3 t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3> VoidActorTaskCaller task(RayFuncVoid5<A, T0, T1, T2, T3> f, T0 t0, T1 t1, ObjectRef<T2> t2, ObjectRef<T3> t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3> VoidActorTaskCaller task(RayFuncVoid5<A, T0, T1, T2, T3> f, T0 t0, ObjectRef<T1> t1, T2 t2, T3 t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3> VoidActorTaskCaller task(RayFuncVoid5<A, T0, T1, T2, T3> f, T0 t0, ObjectRef<T1> t1, T2 t2, ObjectRef<T3> t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3> VoidActorTaskCaller task(RayFuncVoid5<A, T0, T1, T2, T3> f, T0 t0, ObjectRef<T1> t1, ObjectRef<T2> t2, T3 t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3> VoidActorTaskCaller task(RayFuncVoid5<A, T0, T1, T2, T3> f, T0 t0, ObjectRef<T1> t1, ObjectRef<T2> t2, ObjectRef<T3> t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3> VoidActorTaskCaller task(RayFuncVoid5<A, T0, T1, T2, T3> f, ObjectRef<T0> t0, T1 t1, T2 t2, T3 t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3> VoidActorTaskCaller task(RayFuncVoid5<A, T0, T1, T2, T3> f, ObjectRef<T0> t0, T1 t1, T2 t2, ObjectRef<T3> t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3> VoidActorTaskCaller task(RayFuncVoid5<A, T0, T1, T2, T3> f, ObjectRef<T0> t0, T1 t1, ObjectRef<T2> t2, T3 t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3> VoidActorTaskCaller task(RayFuncVoid5<A, T0, T1, T2, T3> f, ObjectRef<T0> t0, T1 t1, ObjectRef<T2> t2, ObjectRef<T3> t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3> VoidActorTaskCaller task(RayFuncVoid5<A, T0, T1, T2, T3> f, ObjectRef<T0> t0, ObjectRef<T1> t1, T2 t2, T3 t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3> VoidActorTaskCaller task(RayFuncVoid5<A, T0, T1, T2, T3> f, ObjectRef<T0> t0, ObjectRef<T1> t1, T2 t2, ObjectRef<T3> t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3> VoidActorTaskCaller task(RayFuncVoid5<A, T0, T1, T2, T3> f, ObjectRef<T0> t0, ObjectRef<T1> t1, ObjectRef<T2> t2, T3 t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3> VoidActorTaskCaller task(RayFuncVoid5<A, T0, T1, T2, T3> f, ObjectRef<T0> t0, ObjectRef<T1> t1, ObjectRef<T2> t2, ObjectRef<T3> t3) {
    Object[] args = new Object[] {t0, t1, t2, t3};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, T2 t2, T3 t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, T2 t2, ObjectRef<T3> t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, T2 t2, ObjectRef<T3> t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, ObjectRef<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, ObjectRef<T2> t2, T3 t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, ObjectRef<T2> t2, ObjectRef<T3> t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, ObjectRef<T2> t2, ObjectRef<T3> t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, ObjectRef<T1> t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, ObjectRef<T1> t1, T2 t2, T3 t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, ObjectRef<T1> t1, T2 t2, ObjectRef<T3> t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, ObjectRef<T1> t1, T2 t2, ObjectRef<T3> t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, ObjectRef<T1> t1, ObjectRef<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, ObjectRef<T1> t1, ObjectRef<T2> t2, T3 t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, ObjectRef<T1> t1, ObjectRef<T2> t2, ObjectRef<T3> t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, ObjectRef<T1> t1, ObjectRef<T2> t2, ObjectRef<T3> t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, ObjectRef<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, ObjectRef<T0> t0, T1 t1, T2 t2, T3 t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, ObjectRef<T0> t0, T1 t1, T2 t2, ObjectRef<T3> t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, ObjectRef<T0> t0, T1 t1, T2 t2, ObjectRef<T3> t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, ObjectRef<T0> t0, T1 t1, ObjectRef<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, ObjectRef<T0> t0, T1 t1, ObjectRef<T2> t2, T3 t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, ObjectRef<T0> t0, T1 t1, ObjectRef<T2> t2, ObjectRef<T3> t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, ObjectRef<T0> t0, T1 t1, ObjectRef<T2> t2, ObjectRef<T3> t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, ObjectRef<T0> t0, ObjectRef<T1> t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, ObjectRef<T0> t0, ObjectRef<T1> t1, T2 t2, T3 t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, ObjectRef<T0> t0, ObjectRef<T1> t1, T2 t2, ObjectRef<T3> t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, ObjectRef<T0> t0, ObjectRef<T1> t1, T2 t2, ObjectRef<T3> t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, ObjectRef<T0> t0, ObjectRef<T1> t1, ObjectRef<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, ObjectRef<T0> t0, ObjectRef<T1> t1, ObjectRef<T2> t2, T3 t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, ObjectRef<T0> t0, ObjectRef<T1> t1, ObjectRef<T2> t2, ObjectRef<T3> t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4, R> ActorTaskCaller<R> task(RayFunc6<A, T0, T1, T2, T3, T4, R> f, ObjectRef<T0> t0, ObjectRef<T1> t1, ObjectRef<T2> t2, ObjectRef<T3> t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new ActorTaskCaller<>((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, T1 t1, T2 t2, T3 t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, T1 t1, T2 t2, ObjectRef<T3> t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, T1 t1, T2 t2, ObjectRef<T3> t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, T1 t1, ObjectRef<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, T1 t1, ObjectRef<T2> t2, T3 t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, T1 t1, ObjectRef<T2> t2, ObjectRef<T3> t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, T1 t1, ObjectRef<T2> t2, ObjectRef<T3> t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, ObjectRef<T1> t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, ObjectRef<T1> t1, T2 t2, T3 t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, ObjectRef<T1> t1, T2 t2, ObjectRef<T3> t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, ObjectRef<T1> t1, T2 t2, ObjectRef<T3> t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, ObjectRef<T1> t1, ObjectRef<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, ObjectRef<T1> t1, ObjectRef<T2> t2, T3 t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, ObjectRef<T1> t1, ObjectRef<T2> t2, ObjectRef<T3> t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, ObjectRef<T1> t1, ObjectRef<T2> t2, ObjectRef<T3> t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, ObjectRef<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, ObjectRef<T0> t0, T1 t1, T2 t2, T3 t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, ObjectRef<T0> t0, T1 t1, T2 t2, ObjectRef<T3> t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, ObjectRef<T0> t0, T1 t1, T2 t2, ObjectRef<T3> t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, ObjectRef<T0> t0, T1 t1, ObjectRef<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, ObjectRef<T0> t0, T1 t1, ObjectRef<T2> t2, T3 t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, ObjectRef<T0> t0, T1 t1, ObjectRef<T2> t2, ObjectRef<T3> t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, ObjectRef<T0> t0, T1 t1, ObjectRef<T2> t2, ObjectRef<T3> t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, ObjectRef<T0> t0, ObjectRef<T1> t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, ObjectRef<T0> t0, ObjectRef<T1> t1, T2 t2, T3 t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, ObjectRef<T0> t0, ObjectRef<T1> t1, T2 t2, ObjectRef<T3> t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, ObjectRef<T0> t0, ObjectRef<T1> t1, T2 t2, ObjectRef<T3> t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, ObjectRef<T0> t0, ObjectRef<T1> t1, ObjectRef<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, ObjectRef<T0> t0, ObjectRef<T1> t1, ObjectRef<T2> t2, T3 t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, ObjectRef<T0> t0, ObjectRef<T1> t1, ObjectRef<T2> t2, ObjectRef<T3> t3, T4 t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

  default <T0, T1, T2, T3, T4> VoidActorTaskCaller task(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, ObjectRef<T0> t0, ObjectRef<T1> t1, ObjectRef<T2> t2, ObjectRef<T3> t3, ObjectRef<T4> t4) {
    Object[] args = new Object[] {t0, t1, t2, t3, t4};
    return new VoidActorTaskCaller((ActorHandle) this, f, args);
  }

}
