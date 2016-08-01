# This code reproduces a memory leak we had in the past

import os
import numpy as np
import ray

ray.init(start_ray_local=True, num_workers=1)

d = {"w": np.zeros(1000000)}

obj_capsule, contained_objrefs = ray.lib.serialize_object(ray.worker.global_worker.handle, d)

while True:
  ray.lib.deserialize_object(ray.worker.global_worker.handle, obj_capsule)
