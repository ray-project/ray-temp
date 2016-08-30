from typing import List, Any
import numpy as np
import ray

__all__ = ["zeros", "zeros_like", "ones", "eye", "dot", "vstack", "hstack", "subarray", "copy", "tril", "triu", "diag", "transpose", "add", "subtract", "sum", "shape", "sum_list"]

@ray.remote()
def zeros(shape, dtype_name="float", order="C"):
  return np.zeros(shape, dtype=np.dtype(dtype_name), order=order)

@ray.remote()
def zeros_like(a, dtype_name="None", order="K", subok=True):
  dtype_val = None if dtype_name == "None" else np.dtype(dtype_name)
  return np.zeros_like(a, dtype=dtype_val, order=order, subok=subok)

@ray.remote()
def ones(shape, dtype_name="float", order="C"):
  return np.ones(shape, dtype=np.dtype(dtype_name), order=order)

@ray.remote()
def eye(N, M=-1, k=0, dtype_name="float"):
  M = N if M == -1 else M
  return np.eye(N, M=M, k=k, dtype=np.dtype(dtype_name))

@ray.remote()
def dot(a, b):
  return np.dot(a, b)

@ray.remote()
def vstack(*xs):
  return np.vstack(xs)

@ray.remote()
def hstack(*xs):
  return np.hstack(xs)

# TODO(rkn): instead of this, consider implementing slicing
@ray.remote()
def subarray(a, lower_indices, upper_indices): # TODO(rkn): be consistent about using "index" versus "indices"
  return a[[slice(l, u) for (l, u) in zip(lower_indices, upper_indices)]]

@ray.remote()
def copy(a, order="K"):
  return np.copy(a, order=order)

@ray.remote()
def tril(m, k=0):
  return np.tril(m, k=k)

@ray.remote()
def triu(m, k=0):
  return np.triu(m, k=k)

@ray.remote()
def diag(v, k=0):
  return np.diag(v, k=k)

@ray.remote()
def transpose(a, axes=[]):
  axes = None if axes == [] else axes
  return np.transpose(a, axes=axes)

@ray.remote()
def add(x1, x2):
  return np.add(x1, x2)

@ray.remote()
def subtract(x1, x2):
  return np.subtract(x1, x2)

@ray.remote()
def sum(x, axis=-1):
  return np.sum(x, axis=axis if axis != -1 else None)

@ray.remote()
def shape(a):
  return np.shape(a)

# We use Any to allow different numerical types as well as numpy arrays.
# TODO(rkn):this isn't in the numpy API, so be careful about exposing this.
@ray.remote()
def sum_list(*xs):
  return np.sum(xs, axis=0)
