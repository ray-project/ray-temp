
#pragma once

#include "ray_object.h"

namespace ray {

template <typename T>
class RayActor : public RayObject<T> {
 public:
  RayActor(UniqueId id) : RayObject<T>(id) {}
  MSGPACK_DEFINE(MSGPACK_BASE(RayObject<T>));
};
}  // namespace ray
