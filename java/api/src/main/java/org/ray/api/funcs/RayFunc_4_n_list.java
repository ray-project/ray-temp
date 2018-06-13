package org.ray.api.funcs;

import java.util.List;
import org.apache.commons.lang3.SerializationUtils;
import org.ray.api.internal.RayFunc;

@FunctionalInterface
public interface RayFunc_4_n_list<T0, T1, T2, T3, R> extends RayFunc {

  List<R> apply(T0 t0, T1 t1, T2 t2, T3 t3) throws Throwable;

}
