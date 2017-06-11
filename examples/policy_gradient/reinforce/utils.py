from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np


# TODO(ekl) this is really hacky, can we do the split inside the graph?
def make_divisible_by(array, n):
  return array[0:array.shape[0] - array.shape[0] % n]


def flatten(weights, start=0, stop=2):
  """This methods reshapes all values in a dictionary.

  The indices from start to stop will be flattened into a single index.

  Args:
    weights: A dictionary mapping keys to numpy arrays.
    start: The starting index.
    stop: The ending index.
  """
  for key, val in weights.items():
    new_shape = val.shape[0:start] + (-1,) + val.shape[stop:]
    weights[key] = val.reshape(new_shape)
  return weights


def concatenate(weights_list):
  keys = weights_list[0].keys()
  result = {}
  for key in keys:
    result[key] = np.concatenate([l[key] for l in weights_list])
  return result


def shuffle(trajectory):
  permutation = np.random.permutation(trajectory["dones"].shape[0])
  for key, val in trajectory.items():
    trajectory[key] = val[permutation]
  return trajectory
