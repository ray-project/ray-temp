from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import pickle
import unittest

import common

ID_SIZE = 20

def random_object_id():
  return common.ObjectID(np.random.bytes(ID_SIZE))

def random_function_id():
  return common.ObjectID(np.random.bytes(ID_SIZE))

def random_task_id():
  return common.ObjectID(np.random.bytes(ID_SIZE))

BASE_SIMPLE_OBJECTS = [
  0, 1, 100000, 0L, 1L, 100000L, 1L << 100, 0.0, 0.5, 0.9, 100000.1, (), [], {},
  "", 990 * "h", u"", 990 * u"h"
]

LIST_SIMPLE_OBJECTS = [[obj] for obj in BASE_SIMPLE_OBJECTS]
TUPLE_SIMPLE_OBJECTS = [(obj,) for obj in BASE_SIMPLE_OBJECTS]
DICT_SIMPLE_OBJECTS = [{(): obj} for obj in BASE_SIMPLE_OBJECTS]

SIMPLE_OBJECTS = (BASE_SIMPLE_OBJECTS +
                  LIST_SIMPLE_OBJECTS +
                  TUPLE_SIMPLE_OBJECTS +
                  DICT_SIMPLE_OBJECTS)

# Create some complex objects that cannot be serialized by value in tasks.

l = []
l.append(l)

class Foo(object):
  def __init__(self):
    pass

BASE_COMPLEX_OBJECTS = [999 * "h", 999 * u"h", l, Foo(), 10 * [10 * [10 * [1]]]]

LIST_COMPLEX_OBJECTS = [[obj] for obj in BASE_COMPLEX_OBJECTS]
TUPLE_COMPLEX_OBJECTS = [(obj,) for obj in BASE_COMPLEX_OBJECTS]
DICT_COMPLEX_OBJECTS = [{(): obj} for obj in BASE_COMPLEX_OBJECTS]

COMPLEX_OBJECTS = (BASE_COMPLEX_OBJECTS +
                   LIST_COMPLEX_OBJECTS +
                   TUPLE_COMPLEX_OBJECTS +
                   DICT_COMPLEX_OBJECTS)

class TestSerialization(unittest.TestCase):

  def test_serialize_by_value(self):

    for val in SIMPLE_OBJECTS:
      self.assertTrue(common.check_simple_value(val))
    for val in COMPLEX_OBJECTS:
      self.assertFalse(common.check_simple_value(val))

class TestObjectID(unittest.TestCase):

  def test_create_object_id(self):
    object_id = random_object_id()

  def test_cannot_pickle_object_ids(self):
    object_ids = [random_object_id() for _ in range(256)]
    def f():
      return object_ids
    def g(val=object_ids):
      return 1
    def h():
      x = object_ids[0]
      return 1
    # Make sure that object IDs cannot be pickled (including functions that
    # close over object IDs).
    self.assertRaises(Exception, lambda : pickling.dumps(object_ids[0]))
    self.assertRaises(Exception, lambda : pickling.dumps(object_ids))
    self.assertRaises(Exception, lambda : pickling.dumps(f))
    self.assertRaises(Exception, lambda : pickling.dumps(g))
    self.assertRaises(Exception, lambda : pickling.dumps(h))

  def test_equality_comparisons(self):
    x1 = common.ObjectID(ID_SIZE * "a")
    x2 = common.ObjectID(ID_SIZE * "a")
    y1 = common.ObjectID(ID_SIZE * "b")
    y2 = common.ObjectID(ID_SIZE * "b")
    self.assertEqual(x1, x2)
    self.assertEqual(y1, y2)
    self.assertNotEqual(x1, y1)

    object_ids1 = [common.ObjectID(ID_SIZE * chr(i)) for i in range(256)]
    object_ids2 = [common.ObjectID(ID_SIZE * chr(i)) for i in range(256)]
    self.assertEqual(len(set(object_ids1)), 256)
    self.assertEqual(len(set(object_ids1 + object_ids2)), 256)
    self.assertEqual(set(object_ids1), set(object_ids2))

  def test_hashability(self):
    x = random_object_id()
    y = random_object_id()
    {x: y}
    set([x, y])

class TestTask(unittest.TestCase):

  def test_create_task(self):
    # TODO(rkn): The function ID should be a FunctionID object, not an ObjectID.
    parent_id = random_task_id()
    function_id = random_function_id()
    object_ids = [random_object_id() for _ in range(256)]
    args_list = [
      [],
      1 * [1],
      10 * [1],
      100 * [1],
      1000 * [1],
      1 * ["a"],
      10 * ["a"],
      100 * ["a"],
      1000 * ["a"],
      [1, 1.3, 2L, 1L << 100, "hi", u"hi", [1, 2]],
      object_ids[:1],
      object_ids[:2],
      object_ids[:3],
      object_ids[:4],
      object_ids[:5],
      object_ids[:10],
      object_ids[:100],
      object_ids[:256],
      [1, object_ids[0]],
      [object_ids[0], "a"],
      [1, object_ids[0], "a"],
      [object_ids[0], 1, object_ids[1], "a"],
      object_ids[:3] + [1, "hi", 2.3] + object_ids[:5],
      object_ids + 100 * ["a"] + object_ids
    ]
    for args in args_list:
      for num_return_vals in [0, 1, 2, 3, 5, 10, 100]:
        task = common.Task(function_id, args, num_return_vals, parent_id, 0)
        self.assertEqual(function_id.id(), task.function_id().id())
        retrieved_args = task.arguments()
        self.assertEqual(num_return_vals, len(task.returns()))
        self.assertEqual(len(args), len(retrieved_args))
        for i in range(len(retrieved_args)):
          if isinstance(retrieved_args[i], common.ObjectID):
            self.assertEqual(retrieved_args[i].id(), args[i].id())
          else:
            self.assertEqual(retrieved_args[i], args[i])

if __name__ == "__main__":
  unittest.main(verbosity=2)
