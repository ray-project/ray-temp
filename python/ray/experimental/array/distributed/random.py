from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import ray.experimental.array.remote as ra
import ray

from .core import DistArray


@ray.task
def normal(shape):
  num_blocks = DistArray.compute_num_blocks(shape)
  objectids = np.empty(num_blocks, dtype=object)
  for index in np.ndindex(*num_blocks):
    objectids[index] = ra.random.normal.remote(
        DistArray.compute_block_shape(index, shape))
  result = DistArray(shape, objectids)
  return result
