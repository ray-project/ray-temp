from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import ray
import sys
import time

if sys.version_info >= (3, 0):
  from importlib import reload

import ray.test.test_functions as test_functions

def relevant_errors(error_type):
  return [info for info in ray.error_info() if info[b"type"] == error_type]

def wait_for_errors(error_type, num_errors, timeout=10):
  start_time = time.time()
  while time.time() - start_time < timeout:
    if len(relevant_errors(error_type)) >= num_errors:
      return
    time.sleep(0.1)
  print("Timing out of wait.")

class FailureTest(unittest.TestCase):
  def testUnknownSerialization(self):
    reload(test_functions)
    ray.init(num_workers=1, driver_mode=ray.SILENT_MODE)

    test_functions.test_unknown_type.remote()
    wait_for_errors(b"task", 1)
    error_info = ray.error_info()
    self.assertEqual(len(relevant_errors(b"task")), 1)

    ray.worker.cleanup()

class TaskSerializationTest(unittest.TestCase):
  def testReturnAndPassUnknownType(self):
    ray.init(num_workers=1, driver_mode=ray.SILENT_MODE)

    class Foo(object):
      pass
    # Check that returning an unknown type from a remote function raises an
    # exception.
    @ray.remote
    def f():
      return Foo()
    self.assertRaises(Exception, lambda : ray.get(f.remote()))
    # Check that passing an unknown type into a remote function raises an
    # exception.
    @ray.remote
    def g(x):
      return 1
    self.assertRaises(Exception, lambda : g.remote(Foo()))

    ray.worker.cleanup()

class TaskStatusTest(unittest.TestCase):
  def testFailedTask(self):
    reload(test_functions)
    ray.init(num_workers=3, driver_mode=ray.SILENT_MODE)

    test_functions.throw_exception_fct1.remote()
    test_functions.throw_exception_fct1.remote()
    wait_for_errors(b"task", 2)
    result = ray.error_info()
    self.assertEqual(len(relevant_errors(b"task")), 2)
    for task in relevant_errors(b"task"):
      self.assertTrue(b"Test function 1 intentionally failed." in task.get(b"message"))

    x = test_functions.throw_exception_fct2.remote()
    try:
      ray.get(x)
    except Exception as e:
      self.assertTrue("Test function 2 intentionally failed." in str(e))
    else:
      self.assertTrue(False) # ray.get should throw an exception

    x, y, z = test_functions.throw_exception_fct3.remote(1.0)
    for ref in [x, y, z]:
      try:
        ray.get(ref)
      except Exception as e:
        self.assertTrue("Test function 3 intentionally failed." in str(e))
      else:
        self.assertTrue(False) # ray.get should throw an exception

    ray.worker.cleanup()

  def testFailImportingRemoteFunction(self):
    ray.init(num_workers=2, driver_mode=ray.SILENT_MODE)

    # This example is somewhat contrived. It should be successfully pickled, and
    # then it should throw an exception when it is unpickled. This may depend a
    # bit on the specifics of our pickler.
    def reducer(*args):
      raise Exception("There is a problem here.")
    class Foo(object):
      def __init__(self):
        self.__name__ = "Foo_object"
        self.func_doc = ""
        self.__globals__ = {}
      def __reduce__(self):
        return reducer, ()
      def __call__(self):
        return
    f = ray.remote(Foo())
    wait_for_errors(b"register_remote_function", 2)
    self.assertTrue(b"There is a problem here." in ray.error_info()[0][b"message"])

    # Check that if we try to call the function it throws an exception and does
    # not hang.
    for _ in range(10):
      self.assertRaises(Exception, lambda : ray.get(f.remote()))

    ray.worker.cleanup()

  def testFailImportingEnvironmentVariable(self):
    ray.init(num_workers=2, driver_mode=ray.SILENT_MODE)

    # This will throw an exception when the environment variable is imported on
    # the workers.
    def initializer():
      if ray.worker.global_worker.mode == ray.WORKER_MODE:
        raise Exception("The initializer failed.")
      return 0
    ray.env.foo = ray.EnvironmentVariable(initializer)
    wait_for_errors(b"register_environment_variable", 2)
    # Check that the error message is in the task info.
    self.assertTrue(b"The initializer failed." in ray.error_info()[0][b"message"])

    ray.worker.cleanup()

  def testFailReinitializingVariable(self):
    ray.init(num_workers=2, driver_mode=ray.SILENT_MODE)

    def initializer():
      return 0
    def reinitializer(foo):
      raise Exception("The reinitializer failed.")
    ray.env.foo = ray.EnvironmentVariable(initializer, reinitializer)
    @ray.remote
    def use_foo():
      ray.env.foo
    use_foo.remote()
    wait_for_errors(b"reinitialize_environment_variable", 1)
    # Check that the error message is in the task info.
    self.assertTrue(b"The reinitializer failed." in ray.error_info()[0][b"message"])

    ray.worker.cleanup()

  def testFailedFunctionToRun(self):
    ray.init(num_workers=2, driver_mode=ray.SILENT_MODE)

    def f(worker):
      if ray.worker.global_worker.mode == ray.WORKER_MODE:
        raise Exception("Function to run failed.")
    ray.worker.global_worker.run_function_on_all_workers(f)
    wait_for_errors(b"function_to_run", 2)
    # Check that the error message is in the task info.
    self.assertEqual(len(ray.error_info()), 2)
    self.assertTrue(b"Function to run failed." in ray.error_info()[0][b"message"])
    self.assertTrue(b"Function to run failed." in ray.error_info()[1][b"message"])

    ray.worker.cleanup()

class ActorTest(unittest.TestCase):

  def testFailedActorInit(self):
    ray.init(num_workers=0, driver_mode=ray.SILENT_MODE)

    error_message1 = "actor constructor failed"
    error_message2 = "actor method failed"
    @ray.actor
    class FailedActor(object):
      def __init__(self):
        raise Exception(error_message1)
      def get_val(self):
        return 1
      def fail_method(self):
        raise Exception(error_message2)

    a = FailedActor()

    # Make sure that we get errors from a failed constructor.
    wait_for_errors(b"task", 1)
    self.assertEqual(len(ray.error_info()), 1)
    self.assertIn(error_message1, ray.error_info()[0][b"message"].decode("ascii"))

    # Make sure that we get errors from a failed method.
    a.fail_method()
    wait_for_errors(b"task", 2)
    self.assertEqual(len(ray.error_info()), 2)
    self.assertIn(error_message2, ray.error_info()[1][b"message"].decode("ascii"))

    ray.worker.cleanup()

  def testIncorrectMethodCalls(self):
    ray.init(num_workers=0, driver_mode=ray.SILENT_MODE)

    @ray.actor
    class Actor(object):
      def __init__(self, missing_variable_name):
        pass
      def get_val(self, x):
        pass

    # Make sure that we get errors if we call the constructor incorrectly.
    # TODO(rkn): These errors should instead be thrown when the method is
    # called.

    # Create an actor with too few arguments.
    a = Actor()
    wait_for_errors(b"task", 1)
    self.assertEqual(len(ray.error_info()), 1)
    self.assertIn("missing_variable_name", ray.error_info()[0][b"message"].decode("ascii"))

    # Create an actor with too many arguments.
    a = Actor(1, 2)
    wait_for_errors(b"task", 2)
    self.assertEqual(len(ray.error_info()), 2)
    self.assertIn("but 3 were given", ray.error_info()[1][b"message"].decode("ascii"))

    # Create an actor the correct number of arguments.
    a = Actor(1)
    # Call a method with too few arguments.
    a.get_val()
    wait_for_errors(b"task", 3)
    self.assertEqual(len(ray.error_info()), 3)
    self.assertIn("missing 1 required", ray.error_info()[2][b"message"].decode("ascii"))
    # Call a method with too many arguments.
    a.get_val(1, 2)
    wait_for_errors(b"task", 4)
    self.assertEqual(len(ray.error_info()), 4)
    self.assertIn("but 3 were given", ray.error_info()[3][b"message"].decode("ascii"))

    ray.worker.cleanup()

if __name__ == "__main__":
  unittest.main(verbosity=2)
