import unittest
import ray
import numpy as np
import time
import subprocess32 as subprocess
import os

import ray.array.remote as ra
import ray.array.distributed as da

class RemoteArrayTest(unittest.TestCase):

  def testMethods(self):
    for module in [ra.core, ra.random, ra.linalg, da.core, da.random, da.linalg]:
      reload(module)
    ray.services.start_ray_local(num_workers=1)

    # test eye
    ref = ra.eye.remote(3)
    val = ray.get(ref)
    self.assertTrue(np.alltrue(val == np.eye(3)))

    # test zeros
    ref = ra.zeros.remote([3, 4, 5])
    val = ray.get(ref)
    self.assertTrue(np.alltrue(val == np.zeros([3, 4, 5])))

    # test qr - pass by value
    val_a = np.random.normal(size=[10, 11])
    ref_q, ref_r = ra.linalg.qr.remote(val_a)
    val_q = ray.get(ref_q)
    val_r = ray.get(ref_r)
    self.assertTrue(np.allclose(np.dot(val_q, val_r), val_a))

    # test qr - pass by objref
    a = ra.random.normal.remote([10, 13])
    ref_q, ref_r = ra.linalg.qr.remote(a)
    val_a = ray.get(a)
    val_q = ray.get(ref_q)
    val_r = ray.get(ref_r)
    self.assertTrue(np.allclose(np.dot(val_q, val_r), val_a))

    ray.services.cleanup()

class DistributedArrayTest(unittest.TestCase):

  def testSerialization(self):
    for module in [ra.core, ra.random, ra.linalg, da.core, da.random, da.linalg]:
      reload(module)
    ray.services.start_ray_local()

    x = da.DistArray()
    x.construct([2, 3, 4], np.array([[[ray.put(0)]]]))
    capsule, _ = ray.serialization.serialize(ray.worker.global_worker.handle, x)
    y = ray.serialization.deserialize(ray.worker.global_worker.handle, capsule)
    self.assertEqual(x.shape, y.shape)
    self.assertEqual(x.objrefs[0, 0, 0].val, y.objrefs[0, 0, 0].val)

    ray.services.cleanup()

  def testAssemble(self):
    for module in [ra.core, ra.random, ra.linalg, da.core, da.random, da.linalg]:
      reload(module)
    ray.services.start_ray_local(num_workers=1)

    a = ra.ones.remote([da.BLOCK_SIZE, da.BLOCK_SIZE])
    b = ra.zeros.remote([da.BLOCK_SIZE, da.BLOCK_SIZE])
    x = da.DistArray()
    x.construct([2 * da.BLOCK_SIZE, da.BLOCK_SIZE], np.array([[a], [b]]))
    self.assertTrue(np.alltrue(x.assemble() == np.vstack([np.ones([da.BLOCK_SIZE, da.BLOCK_SIZE]), np.zeros([da.BLOCK_SIZE, da.BLOCK_SIZE])])))

    ray.services.cleanup()

  def testMethods(self):
    for module in [ra.core, ra.random, ra.linalg, da.core, da.random, da.linalg]:
      reload(module)
    worker_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../scripts/default_worker.py")
    ray.services.start_services_local(num_objstores=2, num_workers_per_objstore=5, worker_path=worker_path)

    x = da.zeros.remote([9, 25, 51], "float")
    self.assertTrue(np.alltrue(ray.get(da.assemble.remote(x)) == np.zeros([9, 25, 51])))

    x = da.ones.remote([11, 25, 49], dtype_name="float")
    self.assertTrue(np.alltrue(ray.get(da.assemble.remote(x)) == np.ones([11, 25, 49])))

    x = da.random.normal.remote([11, 25, 49])
    y = da.copy.remote(x)
    self.assertTrue(np.alltrue(ray.get(da.assemble.remote(x)) == ray.get(da.assemble.remote(y))))

    x = da.eye.remote(25, dtype_name="float")
    self.assertTrue(np.alltrue(ray.get(da.assemble.remote(x)) == np.eye(25)))

    x = da.random.normal.remote([25, 49])
    y = da.triu.remote(x)
    self.assertTrue(np.alltrue(ray.get(da.assemble.remote(y)) == np.triu(ray.get(da.assemble.remote(x)))))

    x = da.random.normal.remote([25, 49])
    y = da.tril.remote(x)
    self.assertTrue(np.alltrue(ray.get(da.assemble.remote(y)) == np.tril(ray.get(da.assemble.remote(x)))))

    x = da.random.normal.remote([25, 49])
    y = da.random.normal.remote([49, 18])
    z = da.dot.remote(x, y)
    w = da.assemble.remote(z)
    u = da.assemble.remote(x)
    v = da.assemble.remote(y)
    np.allclose(ray.get(w), np.dot(ray.get(u), ray.get(v)))
    self.assertTrue(np.allclose(ray.get(w), np.dot(ray.get(u), ray.get(v))))

    # test add
    x = da.random.normal.remote([23, 42])
    y = da.random.normal.remote([23, 42])
    z = da.add.remote(x, y)
    self.assertTrue(np.allclose(ray.get(da.assemble.remote(z)), ray.get(da.assemble.remote(x)) + ray.get(da.assemble.remote(y))))

    # test subtract
    x = da.random.normal.remote([33, 40])
    y = da.random.normal.remote([33, 40])
    z = da.subtract.remote(x, y)
    self.assertTrue(np.allclose(ray.get(da.assemble.remote(z)), ray.get(da.assemble.remote(x)) - ray.get(da.assemble.remote(y))))

    # test transpose
    x = da.random.normal.remote([234, 432])
    y = da.transpose.remote(x)
    self.assertTrue(np.alltrue(ray.get(da.assemble.remote(x)).T == ray.get(da.assemble.remote(y))))

    # test numpy_to_dist
    x = da.random.normal.remote([23, 45])
    y = da.assemble.remote(x)
    z = da.numpy_to_dist.remote(y)
    w = da.assemble.remote(z)
    self.assertTrue(np.alltrue(ray.get(da.assemble.remote(x)) == ray.get(da.assemble.remote(z))))
    self.assertTrue(np.alltrue(ray.get(y) == ray.get(w)))

    # test da.tsqr
    for shape in [[123, da.BLOCK_SIZE], [7, da.BLOCK_SIZE], [da.BLOCK_SIZE, da.BLOCK_SIZE], [da.BLOCK_SIZE, 7], [10 * da.BLOCK_SIZE, da.BLOCK_SIZE]]:
      x = da.random.normal.remote(shape)
      K = min(shape)
      q, r = da.linalg.tsqr.remote(x)
      x_val = ray.get(da.assemble.remote(x))
      q_val = ray.get(da.assemble.remote(q))
      r_val = ray.get(r)
      self.assertTrue(r_val.shape == (K, shape[1]))
      self.assertTrue(np.alltrue(r_val == np.triu(r_val)))
      self.assertTrue(np.allclose(x_val, np.dot(q_val, r_val)))
      self.assertTrue(np.allclose(np.dot(q_val.T, q_val), np.eye(K)))

    # test da.linalg.modified_lu
    def test_modified_lu(d1, d2):
      print "testing dist_modified_lu with d1 = " + str(d1) + ", d2 = " + str(d2)
      assert d1 >= d2
      k = min(d1, d2)
      m = ra.random.normal.remote([d1, d2])
      q, r = ra.linalg.qr.remote(m)
      l, u, s = da.linalg.modified_lu.remote(da.numpy_to_dist.remote(q))
      q_val = ray.get(q)
      r_val = ray.get(r)
      l_val = ray.get(da.assemble.remote(l))
      u_val = ray.get(u)
      s_val = ray.get(s)
      s_mat = np.zeros((d1, d2))
      for i in range(len(s_val)):
        s_mat[i, i] = s_val[i]
      self.assertTrue(np.allclose(q_val - s_mat, np.dot(l_val, u_val))) # check that q - s = l * u
      self.assertTrue(np.alltrue(np.triu(u_val) == u_val)) # check that u is upper triangular
      self.assertTrue(np.alltrue(np.tril(l_val) == l_val)) # check that l is lower triangular

    for d1, d2 in [(100, 100), (99, 98), (7, 5), (7, 7), (20, 7), (20, 10)]:
      test_modified_lu(d1, d2)

    # test dist_tsqr_hr
    def test_dist_tsqr_hr(d1, d2):
      print "testing dist_tsqr_hr with d1 = " + str(d1) + ", d2 = " + str(d2)
      a = da.random.normal.remote([d1, d2])
      y, t, y_top, r = da.linalg.tsqr_hr.remote(a)
      a_val = ray.get(da.assemble.remote(a))
      y_val = ray.get(da.assemble.remote(y))
      t_val = ray.get(t)
      y_top_val = ray.get(y_top)
      r_val = ray.get(r)
      tall_eye = np.zeros((d1, min(d1, d2)))
      np.fill_diagonal(tall_eye, 1)
      q = tall_eye - np.dot(y_val, np.dot(t_val, y_top_val.T))
      self.assertTrue(np.allclose(np.dot(q.T, q), np.eye(min(d1, d2)))) # check that q.T * q = I
      self.assertTrue(np.allclose(np.dot(q, r_val), a_val)) # check that a = (I - y * t * y_top.T) * r

    for d1, d2 in [(123, da.BLOCK_SIZE), (7, da.BLOCK_SIZE), (da.BLOCK_SIZE, da.BLOCK_SIZE), (da.BLOCK_SIZE, 7), (10 * da.BLOCK_SIZE, da.BLOCK_SIZE)]:
      test_dist_tsqr_hr(d1, d2)

    def test_dist_qr(d1, d2):
      print "testing qr with d1 = {}, and d2 = {}.".format(d1, d2)
      a = da.random.normal.remote([d1, d2])
      K = min(d1, d2)
      q, r = da.linalg.qr.remote(a)
      a_val = ray.get(da.assemble.remote(a))
      q_val = ray.get(da.assemble.remote(q))
      r_val = ray.get(da.assemble.remote(r))
      self.assertTrue(q_val.shape == (d1, K))
      self.assertTrue(r_val.shape == (K, d2))
      self.assertTrue(np.allclose(np.dot(q_val.T, q_val), np.eye(K)))
      self.assertTrue(np.alltrue(r_val == np.triu(r_val)))
      self.assertTrue(np.allclose(a_val, np.dot(q_val, r_val)))

    for d1, d2 in [(123, da.BLOCK_SIZE), (7, da.BLOCK_SIZE), (da.BLOCK_SIZE, da.BLOCK_SIZE), (da.BLOCK_SIZE, 7), (13, 21), (34, 35), (8, 7)]:
      test_dist_qr(d1, d2)
      test_dist_qr(d2, d1)
    for _ in range(20):
      d1 = np.random.randint(1, 35)
      d2 = np.random.randint(1, 35)
      test_dist_qr(d1, d2)

    ray.services.cleanup()

if __name__ == "__main__":
    unittest.main()
