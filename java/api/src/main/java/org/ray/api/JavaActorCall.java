// Generated by `RayCallGenerator.java`. DO NOT EDIT.

package org.ray.api;

import org.ray.api.function.RayFunc1;
import org.ray.api.function.RayFunc2;
import org.ray.api.function.RayFunc3;
import org.ray.api.function.RayFunc4;
import org.ray.api.function.RayFunc5;
import org.ray.api.function.RayFunc6;
import org.ray.api.function.RayFuncVoid1;
import org.ray.api.function.RayFuncVoid2;
import org.ray.api.function.RayFuncVoid3;
import org.ray.api.function.RayFuncVoid4;
import org.ray.api.function.RayFuncVoid5;
import org.ray.api.function.RayFuncVoid6;

/**
 * This class provides type-safe interfaces for remote actor calls.
 **/
@SuppressWarnings({"rawtypes", "unchecked"})
interface JavaActorCall<A> {

  default <R> RayObject<R> call(RayFunc1<A, R> f) {
    Object[] args = new Object[]{};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default void call(RayFuncVoid1<A> f) {
    Object[] args = new Object[]{};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, R> RayObject<R> call(RayFunc2<A, T0, R> f, T0 t0) {
    Object[] args = new Object[]{t0};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, R> RayObject<R> call(RayFunc2<A, T0, R> f, RayObject<T0> t0) {
    Object[] args = new Object[]{t0};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0> void call(RayFuncVoid2<A, T0> f, T0 t0) {
    Object[] args = new Object[]{t0};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0> void call(RayFuncVoid2<A, T0> f, RayObject<T0> t0) {
    Object[] args = new Object[]{t0};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, R> RayObject<R> call(RayFunc3<A, T0, T1, R> f, T0 t0, T1 t1) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, R> RayObject<R> call(RayFunc3<A, T0, T1, R> f, T0 t0, RayObject<T1> t1) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, R> RayObject<R> call(RayFunc3<A, T0, T1, R> f, RayObject<T0> t0, T1 t1) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, R> RayObject<R> call(RayFunc3<A, T0, T1, R> f, RayObject<T0> t0, RayObject<T1> t1) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1> void call(RayFuncVoid3<A, T0, T1> f, T0 t0, T1 t1) {
    Object[] args = new Object[]{t0, t1};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1> void call(RayFuncVoid3<A, T0, T1> f, T0 t0, RayObject<T1> t1) {
    Object[] args = new Object[]{t0, t1};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1> void call(RayFuncVoid3<A, T0, T1> f, RayObject<T0> t0, T1 t1) {
    Object[] args = new Object[]{t0, t1};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1> void call(RayFuncVoid3<A, T0, T1> f, RayObject<T0> t0, RayObject<T1> t1) {
    Object[] args = new Object[]{t0, t1};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, R> RayObject<R> call(RayFunc4<A, T0, T1, T2, R> f, T0 t0, T1 t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, R> RayObject<R> call(RayFunc4<A, T0, T1, T2, R> f, T0 t0, T1 t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, R> RayObject<R> call(RayFunc4<A, T0, T1, T2, R> f, T0 t0, RayObject<T1> t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, R> RayObject<R> call(RayFunc4<A, T0, T1, T2, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, R> RayObject<R> call(RayFunc4<A, T0, T1, T2, R> f, RayObject<T0> t0, T1 t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, R> RayObject<R> call(RayFunc4<A, T0, T1, T2, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, R> RayObject<R> call(RayFunc4<A, T0, T1, T2, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, R> RayObject<R> call(RayFunc4<A, T0, T1, T2, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2> void call(RayFuncVoid4<A, T0, T1, T2> f, T0 t0, T1 t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2> void call(RayFuncVoid4<A, T0, T1, T2> f, T0 t0, T1 t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2> void call(RayFuncVoid4<A, T0, T1, T2> f, T0 t0, RayObject<T1> t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2> void call(RayFuncVoid4<A, T0, T1, T2> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2> void call(RayFuncVoid4<A, T0, T1, T2> f, RayObject<T0> t0, T1 t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2> void call(RayFuncVoid4<A, T0, T1, T2> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2> void call(RayFuncVoid4<A, T0, T1, T2> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2> void call(RayFuncVoid4<A, T0, T1, T2> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, T0 t0, T1 t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, T0 t0, T1 t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

  default <T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().callActor(f, (RayJavaActor) this, args);
  }

}
