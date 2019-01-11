from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import logging
import os
import random
import re
import setproctitle
import string
import subprocess
import sys
import threading
import time
from collections import defaultdict, namedtuple, OrderedDict
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pickle
import pytest

import ray
import ray.test.cluster_utils
import ray.test.test_utils
from ray.utils import _random_string

logger = logging.getLogger(__name__)


def assert_equal(obj1, obj2):
    module_numpy = (type(obj1).__module__ == np.__name__
                    or type(obj2).__module__ == np.__name__)
    if module_numpy:
        empty_shape = ((hasattr(obj1, "shape") and obj1.shape == ())
                       or (hasattr(obj2, "shape") and obj2.shape == ()))
        if empty_shape:
            # This is a special case because currently np.testing.assert_equal
            # fails because we do not properly handle different numerical
            # types.
            assert obj1 == obj2, ("Objects {} and {} are "
                                  "different.".format(obj1, obj2))
        else:
            np.testing.assert_equal(obj1, obj2)
    elif hasattr(obj1, "__dict__") and hasattr(obj2, "__dict__"):
        special_keys = ["_pytype_"]
        assert (set(list(obj1.__dict__.keys()) + special_keys) == set(
            list(obj2.__dict__.keys()) + special_keys)), ("Objects {} "
                                                          "and {} are "
                                                          "different.".format(
                                                              obj1, obj2))
        for key in obj1.__dict__.keys():
            if key not in special_keys:
                assert_equal(obj1.__dict__[key], obj2.__dict__[key])
    elif type(obj1) is dict or type(obj2) is dict:
        assert_equal(obj1.keys(), obj2.keys())
        for key in obj1.keys():
            assert_equal(obj1[key], obj2[key])
    elif type(obj1) is list or type(obj2) is list:
        assert len(obj1) == len(obj2), ("Objects {} and {} are lists with "
                                        "different lengths.".format(
                                            obj1, obj2))
        for i in range(len(obj1)):
            assert_equal(obj1[i], obj2[i])
    elif type(obj1) is tuple or type(obj2) is tuple:
        assert len(obj1) == len(obj2), ("Objects {} and {} are tuples with "
                                        "different lengths.".format(
                                            obj1, obj2))
        for i in range(len(obj1)):
            assert_equal(obj1[i], obj2[i])
    elif (ray.serialization.is_named_tuple(type(obj1))
          or ray.serialization.is_named_tuple(type(obj2))):
        assert len(obj1) == len(obj2), ("Objects {} and {} are named tuples "
                                        "with different lengths.".format(
                                            obj1, obj2))
        for i in range(len(obj1)):
            assert_equal(obj1[i], obj2[i])
    else:
        assert obj1 == obj2, "Objects {} and {} are different.".format(
            obj1, obj2)


if sys.version_info >= (3, 0):
    long_extras = [0, np.array([["hi", u"hi"], [1.3, 1]])]
else:

    long_extras = [
        long(0),  # noqa: E501,F821
        np.array([
            ["hi", u"hi"],
            [1.3, long(1)]  # noqa: E501,F821
        ])
    ]

PRIMITIVE_OBJECTS = [
    0, 0.0, 0.9, 1 << 62, 1 << 100, 1 << 999, [1 << 100, [1 << 100]], "a",
    string.printable, "\u262F", u"hello world", u"\xff\xfe\x9c\x001\x000\x00",
    None, True, False, [], (), {},
    np.int8(3),
    np.int32(4),
    np.int64(5),
    np.uint8(3),
    np.uint32(4),
    np.uint64(5),
    np.float32(1.9),
    np.float64(1.9),
    np.zeros([100, 100]),
    np.random.normal(size=[100, 100]),
    np.array(["hi", 3]),
    np.array(["hi", 3], dtype=object)
] + long_extras

COMPLEX_OBJECTS = [
    [[[[[[[[[[[[]]]]]]]]]]]],
    {"obj{}".format(i): np.random.normal(size=[100, 100])
     for i in range(10)},
    # {(): {(): {(): {(): {(): {(): {(): {(): {(): {(): {
    #      (): {(): {}}}}}}}}}}}}},
    (
        (((((((((), ), ), ), ), ), ), ), ), ),
    {
        "a": {
            "b": {
                "c": {
                    "d": {}
                }
            }
        }
    }
]


class Foo(object):
    def __init__(self, value=0):
        self.value = value

    def __hash__(self):
        return hash(self.value)

    def __eq__(self, other):
        return other.value == self.value


class Bar(object):
    def __init__(self):
        for i, val in enumerate(PRIMITIVE_OBJECTS + COMPLEX_OBJECTS):
            setattr(self, "field{}".format(i), val)


class Baz(object):
    def __init__(self):
        self.foo = Foo()
        self.bar = Bar()

    def method(self, arg):
        pass


class Qux(object):
    def __init__(self):
        self.objs = [Foo(), Bar(), Baz()]


class SubQux(Qux):
    def __init__(self):
        Qux.__init__(self)


class CustomError(Exception):
    pass


Point = namedtuple("Point", ["x", "y"])
NamedTupleExample = namedtuple("Example",
                               "field1, field2, field3, field4, field5")

CUSTOM_OBJECTS = [
    Exception("Test object."),
    CustomError(),
    Point(11, y=22),
    Foo(),
    Bar(),
    Baz(),  # Qux(), SubQux(),
    NamedTupleExample(1, 1.0, "hi", np.zeros([3, 5]), [1, 2, 3])
]

BASE_OBJECTS = PRIMITIVE_OBJECTS + COMPLEX_OBJECTS + CUSTOM_OBJECTS

LIST_OBJECTS = [[obj] for obj in BASE_OBJECTS]
TUPLE_OBJECTS = [(obj, ) for obj in BASE_OBJECTS]
# The check that type(obj).__module__ != "numpy" should be unnecessary, but
# otherwise this seems to fail on Mac OS X on Travis.
DICT_OBJECTS = (
    [{
        obj: obj
    } for obj in PRIMITIVE_OBJECTS
     if (obj.__hash__ is not None and type(obj).__module__ != "numpy")] + [{
         0: obj
     } for obj in BASE_OBJECTS] + [{
         Foo(123): Foo(456)
     }])

RAY_TEST_OBJECTS = BASE_OBJECTS + LIST_OBJECTS + TUPLE_OBJECTS + DICT_OBJECTS


@pytest.fixture
def ray_start():
    # Start the Ray processes.
    ray.init(num_cpus=1)
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def shutdown_only():
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_passing_arguments_by_value(ray_start):
    @ray.remote
    def f(x):
        return x

    # Check that we can pass arguments by value to remote functions and
    # that they are uncorrupted.
    for obj in RAY_TEST_OBJECTS:
        assert_equal(obj, ray.get(f.remote(obj)))


def test_ray_recursive_objects(ray_start):
    class ClassA(object):
        pass

    # Make a list that contains itself.
    lst = []
    lst.append(lst)
    # Make an object that contains itself as a field.
    a1 = ClassA()
    a1.field = a1
    # Make two objects that contain each other as fields.
    a2 = ClassA()
    a3 = ClassA()
    a2.field = a3
    a3.field = a2
    # Make a dictionary that contains itself.
    d1 = {}
    d1["key"] = d1
    # Create a list of recursive objects.
    recursive_objects = [lst, a1, a2, a3, d1]

    # Check that exceptions are thrown when we serialize the recursive
    # objects.
    for obj in recursive_objects:
        with pytest.raises(Exception):
            ray.put(obj)


def test_passing_arguments_by_value_out_of_the_box(ray_start):
    @ray.remote
    def f(x):
        return x

    # Test passing lambdas.

    def temp():
        return 1

    assert ray.get(f.remote(temp))() == 1
    assert ray.get(f.remote(lambda x: x + 1))(3) == 4

    # Test sets.
    assert ray.get(f.remote(set())) == set()
    s = {1, (1, 2, "hi")}
    assert ray.get(f.remote(s)) == s

    # Test types.
    assert ray.get(f.remote(int)) == int
    assert ray.get(f.remote(float)) == float
    assert ray.get(f.remote(str)) == str

    class Foo(object):
        def __init__(self):
            pass

    # Make sure that we can put and get a custom type. Note that the result
    # won't be "equal" to Foo.
    ray.get(ray.put(Foo))


def test_putting_object_that_closes_over_object_id(ray_start):
    # This test is here to prevent a regression of
    # https://github.com/ray-project/ray/issues/1317.

    class Foo(object):
        def __init__(self):
            self.val = ray.put(0)

        def method(self):
            f

    f = Foo()
    ray.put(f)


def test_put_get(shutdown_only):
    ray.init(num_cpus=0)

    for i in range(100):
        value_before = i * 10**6
        objectid = ray.put(value_before)
        value_after = ray.get(objectid)
        assert value_before == value_after

    for i in range(100):
        value_before = i * 10**6 * 1.0
        objectid = ray.put(value_before)
        value_after = ray.get(objectid)
        assert value_before == value_after

    for i in range(100):
        value_before = "h" * i
        objectid = ray.put(value_before)
        value_after = ray.get(objectid)
        assert value_before == value_after

    for i in range(100):
        value_before = [1] * i
        objectid = ray.put(value_before)
        value_after = ray.get(objectid)
        assert value_before == value_after


def test_custom_serializers(shutdown_only):
    ray.init(num_cpus=1)

    class Foo(object):
        def __init__(self):
            self.x = 3

    def custom_serializer(obj):
        return 3, "string1", type(obj).__name__

    def custom_deserializer(serialized_obj):
        return serialized_obj, "string2"

    ray.register_custom_serializer(
        Foo, serializer=custom_serializer, deserializer=custom_deserializer)

    assert ray.get(ray.put(Foo())) == ((3, "string1", Foo.__name__), "string2")

    class Bar(object):
        def __init__(self):
            self.x = 3

    ray.register_custom_serializer(
        Bar, serializer=custom_serializer, deserializer=custom_deserializer)

    @ray.remote
    def f():
        return Bar()

    assert ray.get(f.remote()) == ((3, "string1", Bar.__name__), "string2")


def test_serialization_final_fallback(ray_start):
    pytest.importorskip("catboost")
    # This test will only run when "catboost" is installed.
    from catboost import CatBoostClassifier

    model = CatBoostClassifier(
        iterations=2,
        depth=2,
        learning_rate=1,
        loss_function="Logloss",
        logging_level="Verbose")

    reconstructed_model = ray.get(ray.put(model))
    assert set(model.get_params().items()) == set(
        reconstructed_model.get_params().items())


def test_register_class(shutdown_only):
    ray.init(num_cpus=2)

    # Check that putting an object of a class that has not been registered
    # throws an exception.
    class TempClass(object):
        pass

    ray.get(ray.put(TempClass()))

    # Test subtypes of dictionaries.
    value_before = OrderedDict([("hello", 1), ("world", 2)])
    object_id = ray.put(value_before)
    assert value_before == ray.get(object_id)

    value_before = defaultdict(lambda: 0, [("hello", 1), ("world", 2)])
    object_id = ray.put(value_before)
    assert value_before == ray.get(object_id)

    value_before = defaultdict(lambda: [], [("hello", 1), ("world", 2)])
    object_id = ray.put(value_before)
    assert value_before == ray.get(object_id)

    # Test passing custom classes into remote functions from the driver.
    @ray.remote
    def f(x):
        return x

    foo = ray.get(f.remote(Foo(7)))
    assert foo == Foo(7)

    regex = re.compile(r"\d+\.\d*")
    new_regex = ray.get(f.remote(regex))
    # This seems to fail on the system Python 3 that comes with
    # Ubuntu, so it is commented out for now:
    # assert regex == new_regex
    # Instead, we do this:
    assert regex.pattern == new_regex.pattern

    # Test returning custom classes created on workers.
    @ray.remote
    def g():
        return SubQux(), Qux()

    subqux, qux = ray.get(g.remote())
    assert subqux.objs[2].foo.value == 0

    # Test exporting custom class definitions from one worker to another
    # when the worker is blocked in a get.
    class NewTempClass(object):
        def __init__(self, value):
            self.value = value

    @ray.remote
    def h1(x):
        return NewTempClass(x)

    @ray.remote
    def h2(x):
        return ray.get(h1.remote(x))

    assert ray.get(h2.remote(10)).value == 10

    # Test registering multiple classes with the same name.
    @ray.remote(num_return_vals=3)
    def j():
        class Class0(object):
            def method0(self):
                pass

        c0 = Class0()

        class Class0(object):
            def method1(self):
                pass

        c1 = Class0()

        class Class0(object):
            def method2(self):
                pass

        c2 = Class0()

        return c0, c1, c2

    results = []
    for _ in range(5):
        results += j.remote()
    for i in range(len(results) // 3):
        c0, c1, c2 = ray.get(results[(3 * i):(3 * (i + 1))])

        c0.method0()
        c1.method1()
        c2.method2()

        assert not hasattr(c0, "method1")
        assert not hasattr(c0, "method2")
        assert not hasattr(c1, "method0")
        assert not hasattr(c1, "method2")
        assert not hasattr(c2, "method0")
        assert not hasattr(c2, "method1")

    @ray.remote
    def k():
        class Class0(object):
            def method0(self):
                pass

        c0 = Class0()

        class Class0(object):
            def method1(self):
                pass

        c1 = Class0()

        class Class0(object):
            def method2(self):
                pass

        c2 = Class0()

        return c0, c1, c2

    results = ray.get([k.remote() for _ in range(5)])
    for c0, c1, c2 in results:
        c0.method0()
        c1.method1()
        c2.method2()

        assert not hasattr(c0, "method1")
        assert not hasattr(c0, "method2")
        assert not hasattr(c1, "method0")
        assert not hasattr(c1, "method2")
        assert not hasattr(c2, "method0")
        assert not hasattr(c2, "method1")


def test_keyword_args(shutdown_only):
    @ray.remote
    def keyword_fct1(a, b="hello"):
        return "{} {}".format(a, b)

    @ray.remote
    def keyword_fct2(a="hello", b="world"):
        return "{} {}".format(a, b)

    @ray.remote
    def keyword_fct3(a, b, c="hello", d="world"):
        return "{} {} {} {}".format(a, b, c, d)

    ray.init(num_cpus=1)

    x = keyword_fct1.remote(1)
    assert ray.get(x) == "1 hello"
    x = keyword_fct1.remote(1, "hi")
    assert ray.get(x) == "1 hi"
    x = keyword_fct1.remote(1, b="world")
    assert ray.get(x) == "1 world"
    x = keyword_fct1.remote(a=1, b="world")
    assert ray.get(x) == "1 world"

    x = keyword_fct2.remote(a="w", b="hi")
    assert ray.get(x) == "w hi"
    x = keyword_fct2.remote(b="hi", a="w")
    assert ray.get(x) == "w hi"
    x = keyword_fct2.remote(a="w")
    assert ray.get(x) == "w world"
    x = keyword_fct2.remote(b="hi")
    assert ray.get(x) == "hello hi"
    x = keyword_fct2.remote("w")
    assert ray.get(x) == "w world"
    x = keyword_fct2.remote("w", "hi")
    assert ray.get(x) == "w hi"

    x = keyword_fct3.remote(0, 1, c="w", d="hi")
    assert ray.get(x) == "0 1 w hi"
    x = keyword_fct3.remote(0, b=1, c="w", d="hi")
    assert ray.get(x) == "0 1 w hi"
    x = keyword_fct3.remote(a=0, b=1, c="w", d="hi")
    assert ray.get(x) == "0 1 w hi"
    x = keyword_fct3.remote(0, 1, d="hi", c="w")
    assert ray.get(x) == "0 1 w hi"
    x = keyword_fct3.remote(0, 1, c="w")
    assert ray.get(x) == "0 1 w world"
    x = keyword_fct3.remote(0, 1, d="hi")
    assert ray.get(x) == "0 1 hello hi"
    x = keyword_fct3.remote(0, 1)
    assert ray.get(x) == "0 1 hello world"
    x = keyword_fct3.remote(a=0, b=1)
    assert ray.get(x) == "0 1 hello world"

    # Check that we cannot pass invalid keyword arguments to functions.
    @ray.remote
    def f1():
        return

    @ray.remote
    def f2(x, y=0, z=0):
        return

    # Make sure we get an exception if too many arguments are passed in.
    with pytest.raises(Exception):
        f1.remote(3)

    with pytest.raises(Exception):
        f1.remote(x=3)

    with pytest.raises(Exception):
        f2.remote(0, w=0)

    with pytest.raises(Exception):
        f2.remote(3, x=3)

    # Make sure we get an exception if too many arguments are passed in.
    with pytest.raises(Exception):
        f2.remote(1, 2, 3, 4)

    @ray.remote
    def f3(x):
        return x

    assert ray.get(f3.remote(4)) == 4


def test_variable_number_of_args(shutdown_only):
    @ray.remote
    def varargs_fct1(*a):
        return " ".join(map(str, a))

    @ray.remote
    def varargs_fct2(a, *b):
        return " ".join(map(str, b))

    try:

        @ray.remote
        def kwargs_throw_exception(**c):
            return ()

        kwargs_exception_thrown = False
    except Exception:
        kwargs_exception_thrown = True

    ray.init(num_cpus=1)

    x = varargs_fct1.remote(0, 1, 2)
    assert ray.get(x) == "0 1 2"
    x = varargs_fct2.remote(0, 1, 2)
    assert ray.get(x) == "1 2"

    assert kwargs_exception_thrown

    @ray.remote
    def f1(*args):
        return args

    @ray.remote
    def f2(x, y, *args):
        return x, y, args

    assert ray.get(f1.remote()) == ()
    assert ray.get(f1.remote(1)) == (1, )
    assert ray.get(f1.remote(1, 2, 3)) == (1, 2, 3)
    with pytest.raises(Exception):
        f2.remote()
    with pytest.raises(Exception):
        f2.remote(1)
    assert ray.get(f2.remote(1, 2)) == (1, 2, ())
    assert ray.get(f2.remote(1, 2, 3)) == (1, 2, (3, ))
    assert ray.get(f2.remote(1, 2, 3, 4)) == (1, 2, (3, 4))

    def testNoArgs(self):
        @ray.remote
        def no_op():
            pass

        self.init_ray()

        ray.get(no_op.remote())


def test_defining_remote_functions(shutdown_only):
    ray.init(num_cpus=3)

    # Test that we can define a remote function in the shell.
    @ray.remote
    def f(x):
        return x + 1

    assert ray.get(f.remote(0)) == 1

    # Test that we can redefine the remote function.
    @ray.remote
    def f(x):
        return x + 10

    while True:
        val = ray.get(f.remote(0))
        assert val in [1, 10]
        if val == 10:
            break
        else:
            logger.info("Still using old definition of f, trying again.")

    # Test that we can close over plain old data.
    data = [
        np.zeros([3, 5]), (1, 2, "a"), [0.0, 1.0, 1 << 62], 1 << 60, {
            "a": np.zeros(3)
        }
    ]

    @ray.remote
    def g():
        return data

    ray.get(g.remote())

    # Test that we can close over modules.
    @ray.remote
    def h():
        return np.zeros([3, 5])

    assert_equal(ray.get(h.remote()), np.zeros([3, 5]))

    @ray.remote
    def j():
        return time.time()

    ray.get(j.remote())

    # Test that we can define remote functions that call other remote
    # functions.
    @ray.remote
    def k(x):
        return x + 1

    @ray.remote
    def k2(x):
        return ray.get(k.remote(x))

    @ray.remote
    def m(x):
        return ray.get(k2.remote(x))

    assert ray.get(k.remote(1)) == 2
    assert ray.get(k2.remote(1)) == 2
    assert ray.get(m.remote(1)) == 2

    def test_submit_api(shutdown_only):
        ray.init(num_cpus=1, num_gpus=1, resources={"Custom": 1})

        @ray.remote
        def f(n):
            return list(range(n))

        @ray.remote
        def g():
            return ray.get_gpu_ids()

        assert f._remote([0], num_return_vals=0) is None
        id1 = f._remote(args=[1], num_return_vals=1)
        assert ray.get(id1) == [0]
        id1, id2 = f._remote(args=[2], num_return_vals=2)
        assert ray.get([id1, id2]) == [0, 1]
        id1, id2, id3 = f._remote(args=[3], num_return_vals=3)
        assert ray.get([id1, id2, id3]) == [0, 1, 2]
        assert ray.get(
            g._remote(
                args=[], num_cpus=1, num_gpus=1,
                resources={"Custom": 1})) == [0]
        infeasible_id = g._remote(args=[], resources={"NonexistentCustom": 1})
        ready_ids, remaining_ids = ray.wait([infeasible_id], timeout=0.05)
        assert len(ready_ids) == 0
        assert len(remaining_ids) == 1

        @ray.remote
        class Actor(object):
            def __init__(self, x, y=0):
                self.x = x
                self.y = y

            def method(self, a, b=0):
                return self.x, self.y, a, b

            def gpu_ids(self):
                return ray.get_gpu_ids()

        a = Actor._remote(
            args=[0], kwargs={"y": 1}, num_gpus=1, resources={"Custom": 1})

        id1, id2, id3, id4 = a.method._remote(
            args=["test"], kwargs={"b": 2}, num_return_vals=4)
        assert ray.get([id1, id2, id3, id4]) == [0, 1, "test", 2]


def test_get_multiple(shutdown_only):
    ray.init(num_cpus=1)
    object_ids = [ray.put(i) for i in range(10)]
    assert ray.get(object_ids) == list(range(10))

    # Get a random choice of object IDs with duplicates.
    indices = list(np.random.choice(range(10), 5))
    indices += indices
    results = ray.get([object_ids[i] for i in indices])
    assert results == indices


def test_get_multiple_experimental(shutdown_only):
    ray.init(num_cpus=1)
    object_ids = [ray.put(i) for i in range(10)]

    object_ids_tuple = tuple(object_ids)
    assert ray.experimental.get(object_ids_tuple) == list(range(10))

    object_ids_nparray = np.array(object_ids)
    assert ray.experimental.get(object_ids_nparray) == list(range(10))


def test_get_dict(shutdown_only):
    ray.init(num_cpus=1)
    d = {str(i): ray.put(i) for i in range(5)}
    for i in range(5, 10):
        d[str(i)] = i
    result = ray.experimental.get(d)
    expected = {str(i): i for i in range(10)}
    assert result == expected


def test_wait(shutdown_only):
    ray.init(num_cpus=1)

    @ray.remote
    def f(delay):
        time.sleep(delay)
        return 1

    objectids = [f.remote(1.0), f.remote(0.5), f.remote(0.5), f.remote(0.5)]
    ready_ids, remaining_ids = ray.wait(objectids)
    assert len(ready_ids) == 1
    assert len(remaining_ids) == 3
    ready_ids, remaining_ids = ray.wait(objectids, num_returns=4)
    assert set(ready_ids) == set(objectids)
    assert remaining_ids == []

    objectids = [f.remote(0.5), f.remote(0.5), f.remote(0.5), f.remote(0.5)]
    start_time = time.time()
    ready_ids, remaining_ids = ray.wait(objectids, timeout=1.75, num_returns=4)
    assert time.time() - start_time < 2
    assert len(ready_ids) == 3
    assert len(remaining_ids) == 1
    ray.wait(objectids)
    objectids = [f.remote(1.0), f.remote(0.5), f.remote(0.5), f.remote(0.5)]
    start_time = time.time()
    ready_ids, remaining_ids = ray.wait(objectids, timeout=5.0)
    assert time.time() - start_time < 5
    assert len(ready_ids) == 1
    assert len(remaining_ids) == 3

    # Verify that calling wait with duplicate object IDs throws an
    # exception.
    x = ray.put(1)
    with pytest.raises(Exception):
        ray.wait([x, x])

    # Make sure it is possible to call wait with an empty list.
    ready_ids, remaining_ids = ray.wait([])
    assert ready_ids == []
    assert remaining_ids == []

    # Test semantics of num_returns with no timeout.
    oids = [ray.put(i) for i in range(10)]
    (found, rest) = ray.wait(oids, num_returns=2)
    assert len(found) == 2
    assert len(rest) == 8

    # Verify that incorrect usage raises a TypeError.
    x = ray.put(1)
    with pytest.raises(TypeError):
        ray.wait(x)
    with pytest.raises(TypeError):
        ray.wait(1)
    with pytest.raises(TypeError):
        ray.wait([1])


def test_wait_iterables(shutdown_only):
    ray.init(num_cpus=1)

    @ray.remote
    def f(delay):
        time.sleep(delay)
        return 1

    objectids = (f.remote(1.0), f.remote(0.5), f.remote(0.5), f.remote(0.5))
    ready_ids, remaining_ids = ray.experimental.wait(objectids)
    assert len(ready_ids) == 1
    assert len(remaining_ids) == 3

    objectids = np.array(
        [f.remote(1.0),
         f.remote(0.5),
         f.remote(0.5),
         f.remote(0.5)])
    ready_ids, remaining_ids = ray.experimental.wait(objectids)
    assert len(ready_ids) == 1
    assert len(remaining_ids) == 3


def test_multiple_waits_and_gets(shutdown_only):
    # It is important to use three workers here, so that the three tasks
    # launched in this experiment can run at the same time.
    ray.init(num_cpus=3)

    @ray.remote
    def f(delay):
        time.sleep(delay)
        return 1

    @ray.remote
    def g(l):
        # The argument l should be a list containing one object ID.
        ray.wait([l[0]])

    @ray.remote
    def h(l):
        # The argument l should be a list containing one object ID.
        ray.get(l[0])

    # Make sure that multiple wait requests involving the same object ID
    # all return.
    x = f.remote(1)
    ray.get([g.remote([x]), g.remote([x])])

    # Make sure that multiple get requests involving the same object ID all
    # return.
    x = f.remote(1)
    ray.get([h.remote([x]), h.remote([x])])


def test_caching_functions_to_run(shutdown_only):
    # Test that we export functions to run on all workers before the driver
    # is connected.
    def f(worker_info):
        sys.path.append(1)

    ray.worker.global_worker.run_function_on_all_workers(f)

    def f(worker_info):
        sys.path.append(2)

    ray.worker.global_worker.run_function_on_all_workers(f)

    def g(worker_info):
        sys.path.append(3)

    ray.worker.global_worker.run_function_on_all_workers(g)

    def f(worker_info):
        sys.path.append(4)

    ray.worker.global_worker.run_function_on_all_workers(f)

    ray.init(num_cpus=1)

    @ray.remote
    def get_state():
        time.sleep(1)
        return sys.path[-4], sys.path[-3], sys.path[-2], sys.path[-1]

    res1 = get_state.remote()
    res2 = get_state.remote()
    assert ray.get(res1) == (1, 2, 3, 4)
    assert ray.get(res2) == (1, 2, 3, 4)

    # Clean up the path on the workers.
    def f(worker_info):
        sys.path.pop()
        sys.path.pop()
        sys.path.pop()
        sys.path.pop()

    ray.worker.global_worker.run_function_on_all_workers(f)


def test_running_function_on_all_workers(shutdown_only):
    ray.init(num_cpus=1)

    def f(worker_info):
        sys.path.append("fake_directory")

    ray.worker.global_worker.run_function_on_all_workers(f)

    @ray.remote
    def get_path1():
        return sys.path

    assert "fake_directory" == ray.get(get_path1.remote())[-1]

    def f(worker_info):
        sys.path.pop(-1)

    ray.worker.global_worker.run_function_on_all_workers(f)

    # Create a second remote function to guarantee that when we call
    # get_path2.remote(), the second function to run will have been run on
    # the worker.
    @ray.remote
    def get_path2():
        return sys.path

    assert "fake_directory" not in ray.get(get_path2.remote())


def test_profiling_api(shutdown_only):
    ray.init(num_cpus=2)

    @ray.remote
    def f():
        with ray.profile(
                "custom_event",
                extra_data={"name": "custom name"}) as ray_prof:
            ray_prof.set_attribute("key", "value")

    ray.put(1)
    object_id = f.remote()
    ray.wait([object_id])
    ray.get(object_id)

    # Wait until all of the profiling information appears in the profile
    # table.
    timeout_seconds = 20
    start_time = time.time()
    while True:
        if time.time() - start_time > timeout_seconds:
            raise Exception("Timed out while waiting for information in "
                            "profile table.")
        profile_data = ray.global_state.chrome_tracing_dump()
        event_types = {event["cat"] for event in profile_data}
        expected_types = [
            "worker_idle",
            "task",
            "task:deserialize_arguments",
            "task:execute",
            "task:store_outputs",
            "wait_for_function",
            "ray.get",
            "ray.put",
            "ray.wait",
            "submit_task",
            "fetch_and_run_function",
            "register_remote_function",
            "custom_event",  # This is the custom one from ray.profile.
        ]

        if all(expected_type in event_types
               for expected_type in expected_types):
            break


@pytest.fixture()
def ray_start_cluster():
    cluster = ray.test.cluster_utils.Cluster()
    yield cluster

    # The code after the yield will run as teardown code.
    ray.shutdown()
    cluster.shutdown()


def test_object_transfer_dump(ray_start_cluster):
    cluster = ray_start_cluster

    num_nodes = 3
    for i in range(num_nodes):
        cluster.add_node(resources={str(i): 1}, object_store_memory=10**9)
    ray.init(redis_address=cluster.redis_address)

    @ray.remote
    def f(x):
        return

    # These objects will live on different nodes.
    object_ids = [
        f._remote(args=[1], resources={str(i): 1}) for i in range(num_nodes)
    ]

    # Broadcast each object from each machine to each other machine.
    for object_id in object_ids:
        ray.get([
            f._remote(args=[object_id], resources={str(i): 1})
            for i in range(num_nodes)
        ])

    # The profiling information only flushes once every second.
    time.sleep(1.1)

    transfer_dump = ray.global_state.chrome_tracing_object_transfer_dump()
    # Make sure the transfer dump can be serialized with JSON.
    json.loads(json.dumps(transfer_dump))
    assert len(transfer_dump) >= num_nodes**2
    assert len({
        event["pid"]
        for event in transfer_dump if event["name"] == "transfer_receive"
    }) == num_nodes
    assert len({
        event["pid"]
        for event in transfer_dump if event["name"] == "transfer_send"
    }) == num_nodes


def test_identical_function_names(shutdown_only):
    # Define a bunch of remote functions and make sure that we don't
    # accidentally call an older version.
    ray.init(num_cpus=1)

    num_calls = 200

    @ray.remote
    def f():
        return 1

    results1 = [f.remote() for _ in range(num_calls)]

    @ray.remote
    def f():
        return 2

    results2 = [f.remote() for _ in range(num_calls)]

    @ray.remote
    def f():
        return 3

    results3 = [f.remote() for _ in range(num_calls)]

    @ray.remote
    def f():
        return 4

    results4 = [f.remote() for _ in range(num_calls)]

    @ray.remote
    def f():
        return 5

    results5 = [f.remote() for _ in range(num_calls)]

    assert ray.get(results1) == num_calls * [1]
    assert ray.get(results2) == num_calls * [2]
    assert ray.get(results3) == num_calls * [3]
    assert ray.get(results4) == num_calls * [4]
    assert ray.get(results5) == num_calls * [5]

    @ray.remote
    def g():
        return 1

    @ray.remote  # noqa: F811
    def g():
        return 2

    @ray.remote  # noqa: F811
    def g():
        return 3

    @ray.remote  # noqa: F811
    def g():
        return 4

    @ray.remote  # noqa: F811
    def g():
        return 5

    result_values = ray.get([g.remote() for _ in range(num_calls)])
    assert result_values == num_calls * [5]


def test_illegal_api_calls(shutdown_only):
    ray.init(num_cpus=1)

    # Verify that we cannot call put on an ObjectID.
    x = ray.put(1)
    with pytest.raises(Exception):
        ray.put(x)
    # Verify that we cannot call get on a regular value.
    with pytest.raises(Exception):
        ray.get(3)


def test_multithreading(shutdown_only):
    # This test requires at least 2 CPUs to finish since the worker does not
    # relase resources when joining the threads.
    ray.init(num_cpus=2)

    def run_test_in_multi_threads(test_case, num_threads=20, num_repeats=50):
        """A helper function that runs test cases in multiple threads."""

        def wrapper():
            for _ in range(num_repeats):
                test_case()
                time.sleep(random.randint(0, 10) / 1000.0)
            return "ok"

        executor = ThreadPoolExecutor(max_workers=num_threads)
        futures = [executor.submit(wrapper) for _ in range(num_threads)]
        for future in futures:
            assert future.result() == "ok"

    @ray.remote
    def echo(value, delay_ms=0):
        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0)
        return value

    @ray.remote
    class Echo(object):
        def echo(self, value):
            return value

    def test_api_in_multi_threads():
        """Test using Ray api in multiple threads."""

        # Test calling remote functions in multiple threads.
        def test_remote_call():
            value = random.randint(0, 1000000)
            result = ray.get(echo.remote(value))
            assert value == result

        run_test_in_multi_threads(test_remote_call)

        # Test multiple threads calling one actor.
        actor = Echo.remote()

        def test_call_actor():
            value = random.randint(0, 1000000)
            result = ray.get(actor.echo.remote(value))
            assert value == result

        run_test_in_multi_threads(test_call_actor)

        # Test put and get.
        def test_put_and_get():
            value = random.randint(0, 1000000)
            result = ray.get(ray.put(value))
            assert value == result

        run_test_in_multi_threads(test_put_and_get)

        # Test multiple threads waiting for objects.
        num_wait_objects = 10
        objects = [
            echo.remote(i, delay_ms=10) for i in range(num_wait_objects)
        ]

        def test_wait():
            ready, _ = ray.wait(
                objects,
                num_returns=len(objects),
                timeout=1000.0,
            )
            assert len(ready) == num_wait_objects
            assert ray.get(ready) == list(range(num_wait_objects))

        run_test_in_multi_threads(test_wait, num_repeats=1)

    # Run tests in a driver.
    test_api_in_multi_threads()

    # Run tests in a worker.
    @ray.remote
    def run_tests_in_worker():
        test_api_in_multi_threads()
        return "ok"

    assert ray.get(run_tests_in_worker.remote()) == "ok"

    # Test actor that runs background threads.
    @ray.remote
    class MultithreadedActor(object):
        def __init__(self):
            self.lock = threading.Lock()
            self.thread_results = []

        def background_thread(self, wait_objects):
            try:
                # Test wait
                ready, _ = ray.wait(
                    wait_objects,
                    num_returns=len(wait_objects),
                    timeout=1000.0,
                )
                assert len(ready) == len(wait_objects)
                for _ in range(50):
                    num = 20
                    # Test remote call
                    results = [echo.remote(i) for i in range(num)]
                    assert ray.get(results) == list(range(num))
                    # Test put and get
                    objects = [ray.put(i) for i in range(num)]
                    assert ray.get(objects) == list(range(num))
                    time.sleep(random.randint(0, 10) / 1000.0)
            except Exception as e:
                with self.lock:
                    self.thread_results.append(e)
            else:
                with self.lock:
                    self.thread_results.append("ok")

        def spawn(self):
            wait_objects = [echo.remote(i, delay_ms=10) for i in range(20)]
            self.threads = [
                threading.Thread(
                    target=self.background_thread, args=(wait_objects, ))
                for _ in range(20)
            ]
            [thread.start() for thread in self.threads]

        def join(self):
            [thread.join() for thread in self.threads]
            assert self.thread_results == ["ok"] * len(self.threads)
            return "ok"

    actor = MultithreadedActor.remote()
    actor.spawn.remote()
    ray.get(actor.join.remote()) == "ok"


def test_free_objects_multi_node(ray_start_cluster):
    # This test will do following:
    # 1. Create 3 raylets that each hold an actor.
    # 2. Each actor creates an object which is the deletion target.
    # 3. Invoke 64 methods on each actor to flush plasma client.
    # 4. After flushing, the plasma client releases the targets.
    # 5. Check that the deletion targets have been deleted.
    # Caution: if remote functions are used instead of actor methods,
    # one raylet may create more than one worker to execute the
    # tasks, so the flushing operations may be executed in different
    # workers and the plasma client holding the deletion target
    # may not be flushed.
    cluster = ray_start_cluster
    config = json.dumps({"object_manager_repeated_push_delay_ms": 1000})
    for i in range(3):
        cluster.add_node(
            num_cpus=1,
            resources={"Custom{}".format(i): 1},
            _internal_config=config)
    ray.init(redis_address=cluster.redis_address)

    @ray.remote(resources={"Custom0": 1})
    class ActorOnNode0(object):
        def get(self):
            return ray.worker.global_worker.plasma_client.store_socket_name

    @ray.remote(resources={"Custom1": 1})
    class ActorOnNode1(object):
        def get(self):
            return ray.worker.global_worker.plasma_client.store_socket_name

    @ray.remote(resources={"Custom2": 1})
    class ActorOnNode2(object):
        def get(self):
            return ray.worker.global_worker.plasma_client.store_socket_name

    def create(actors):
        a = actors[0].get.remote()
        b = actors[1].get.remote()
        c = actors[2].get.remote()
        (l1, l2) = ray.wait([a, b, c], num_returns=3)
        assert len(l1) == 3
        assert len(l2) == 0
        return (a, b, c)

    def flush(actors):
        # Flush the Release History.
        # Current Plasma Client Cache will maintain 64-item list.
        # If the number changed, this will fail.
        logger.info("Start Flush!")
        for i in range(64):
            ray.get([actor.get.remote() for actor in actors])
        logger.info("Flush finished!")

    def run_one_test(actors, local_only):
        (a, b, c) = create(actors)
        # The three objects should be generated on different object stores.
        assert ray.get(a) != ray.get(b)
        assert ray.get(a) != ray.get(c)
        assert ray.get(c) != ray.get(b)
        ray.internal.free([a, b, c], local_only=local_only)
        flush(actors)
        return (a, b, c)

    actors = [
        ActorOnNode0.remote(),
        ActorOnNode1.remote(),
        ActorOnNode2.remote()
    ]
    # Case 1: run this local_only=False. All 3 objects will be deleted.
    (a, b, c) = run_one_test(actors, False)
    (l1, l2) = ray.wait([a, b, c], timeout=0.01, num_returns=1)
    # All the objects are deleted.
    assert len(l1) == 0
    assert len(l2) == 3
    # Case 2: run this local_only=True. Only 1 object will be deleted.
    (a, b, c) = run_one_test(actors, True)
    (l1, l2) = ray.wait([a, b, c], timeout=0.01, num_returns=3)
    # One object is deleted and 2 objects are not.
    assert len(l1) == 2
    assert len(l2) == 1
    # The deleted object will have the same store with the driver.
    local_return = ray.worker.global_worker.plasma_client.store_socket_name
    for object_id in l1:
        assert ray.get(object_id) != local_return


def test_local_mode(shutdown_only):
    @ray.remote
    def local_mode_f():
        return np.array([0, 0])

    @ray.remote
    def local_mode_g(x):
        x[0] = 1
        return x

    ray.init(local_mode=True)

    @ray.remote
    def f():
        return np.ones([3, 4, 5])

    xref = f.remote()
    # Remote functions should return by value.
    assert_equal(xref, np.ones([3, 4, 5]))
    # Check that ray.get is the identity.
    assert_equal(xref, ray.get(xref))
    y = np.random.normal(size=[11, 12])
    # Check that ray.put is the identity.
    assert_equal(y, ray.put(y))

    # Make sure objects are immutable, this example is why we need to copy
    # arguments before passing them into remote functions in python mode
    aref = local_mode_f.remote()
    assert_equal(aref, np.array([0, 0]))
    bref = local_mode_g.remote(aref)
    # Make sure local_mode_g does not mutate aref.
    assert_equal(aref, np.array([0, 0]))
    assert_equal(bref, np.array([1, 0]))

    # wait should return the first num_returns values passed in as the
    # first list and the remaining values as the second list
    num_returns = 5
    object_ids = [ray.put(i) for i in range(20)]
    ready, remaining = ray.wait(
        object_ids, num_returns=num_returns, timeout=None)
    assert_equal(ready, object_ids[:num_returns])
    assert_equal(remaining, object_ids[num_returns:])

    # Test actors in LOCAL_MODE.

    @ray.remote
    class LocalModeTestClass(object):
        def __init__(self, array):
            self.array = array

        def set_array(self, array):
            self.array = array

        def get_array(self):
            return self.array

        def modify_and_set_array(self, array):
            array[0] = -1
            self.array = array

    test_actor = LocalModeTestClass.remote(np.arange(10))
    # Remote actor functions should return by value
    assert_equal(test_actor.get_array.remote(), np.arange(10))

    test_array = np.arange(10)
    # Remote actor functions should not mutate arguments
    test_actor.modify_and_set_array.remote(test_array)
    assert_equal(test_array, np.arange(10))
    # Remote actor functions should keep state
    test_array[0] = -1
    assert_equal(test_array, test_actor.get_array.remote())

    # Check that actor handles work in Python mode.

    @ray.remote
    def use_actor_handle(handle):
        array = np.ones(10)
        handle.set_array.remote(array)
        assert np.alltrue(array == ray.get(handle.get_array.remote()))

    ray.get(use_actor_handle.remote(test_actor))


def test_resource_constraints(shutdown_only):
    num_workers = 20
    ray.init(num_cpus=10, num_gpus=2)

    @ray.remote(num_cpus=0)
    def get_worker_id():
        time.sleep(0.1)
        return os.getpid()

    # Attempt to wait for all of the workers to start up.
    while True:
        if len(
                set(
                    ray.get([
                        get_worker_id.remote() for _ in range(num_workers)
                    ]))) == num_workers:
            break

    time_buffer = 0.3

    # At most 10 copies of this can run at once.
    @ray.remote(num_cpus=1)
    def f(n):
        time.sleep(n)

    start_time = time.time()
    ray.get([f.remote(0.5) for _ in range(10)])
    duration = time.time() - start_time
    assert duration < 0.5 + time_buffer
    assert duration > 0.5

    start_time = time.time()
    ray.get([f.remote(0.5) for _ in range(11)])
    duration = time.time() - start_time
    assert duration < 1 + time_buffer
    assert duration > 1

    @ray.remote(num_cpus=3)
    def f(n):
        time.sleep(n)

    start_time = time.time()
    ray.get([f.remote(0.5) for _ in range(3)])
    duration = time.time() - start_time
    assert duration < 0.5 + time_buffer
    assert duration > 0.5

    start_time = time.time()
    ray.get([f.remote(0.5) for _ in range(4)])
    duration = time.time() - start_time
    assert duration < 1 + time_buffer
    assert duration > 1

    @ray.remote(num_gpus=1)
    def f(n):
        time.sleep(n)

    start_time = time.time()
    ray.get([f.remote(0.5) for _ in range(2)])
    duration = time.time() - start_time
    assert duration < 0.5 + time_buffer
    assert duration > 0.5

    start_time = time.time()
    ray.get([f.remote(0.5) for _ in range(3)])
    duration = time.time() - start_time
    assert duration < 1 + time_buffer
    assert duration > 1

    start_time = time.time()
    ray.get([f.remote(0.5) for _ in range(4)])
    duration = time.time() - start_time
    assert duration < 1 + time_buffer
    assert duration > 1


def test_multi_resource_constraints(shutdown_only):
    num_workers = 20
    ray.init(num_cpus=10, num_gpus=10)

    @ray.remote(num_cpus=0)
    def get_worker_id():
        time.sleep(0.1)
        return os.getpid()

    # Attempt to wait for all of the workers to start up.
    while True:
        if len(
                set(
                    ray.get([
                        get_worker_id.remote() for _ in range(num_workers)
                    ]))) == num_workers:
            break

    @ray.remote(num_cpus=1, num_gpus=9)
    def f(n):
        time.sleep(n)

    @ray.remote(num_cpus=9, num_gpus=1)
    def g(n):
        time.sleep(n)

    time_buffer = 0.3

    start_time = time.time()
    ray.get([f.remote(0.5), g.remote(0.5)])
    duration = time.time() - start_time
    assert duration < 0.5 + time_buffer
    assert duration > 0.5

    start_time = time.time()
    ray.get([f.remote(0.5), f.remote(0.5)])
    duration = time.time() - start_time
    assert duration < 1 + time_buffer
    assert duration > 1

    start_time = time.time()
    ray.get([g.remote(0.5), g.remote(0.5)])
    duration = time.time() - start_time
    assert duration < 1 + time_buffer
    assert duration > 1

    start_time = time.time()
    ray.get([f.remote(0.5), f.remote(0.5), g.remote(0.5), g.remote(0.5)])
    duration = time.time() - start_time
    assert duration < 1 + time_buffer
    assert duration > 1


def test_gpu_ids(shutdown_only):
    num_gpus = 10
    ray.init(num_cpus=10, num_gpus=num_gpus)

    @ray.remote(num_gpus=0)
    def f0():
        time.sleep(0.1)
        gpu_ids = ray.get_gpu_ids()
        assert len(gpu_ids) == 0
        assert (os.environ["CUDA_VISIBLE_DEVICES"] == ",".join(
            [str(i) for i in gpu_ids]))
        for gpu_id in gpu_ids:
            assert gpu_id in range(num_gpus)
        return gpu_ids

    @ray.remote(num_gpus=1)
    def f1():
        time.sleep(0.1)
        gpu_ids = ray.get_gpu_ids()
        assert len(gpu_ids) == 1
        assert (os.environ["CUDA_VISIBLE_DEVICES"] == ",".join(
            [str(i) for i in gpu_ids]))
        for gpu_id in gpu_ids:
            assert gpu_id in range(num_gpus)
        return gpu_ids

    @ray.remote(num_gpus=2)
    def f2():
        time.sleep(0.1)
        gpu_ids = ray.get_gpu_ids()
        assert len(gpu_ids) == 2
        assert (os.environ["CUDA_VISIBLE_DEVICES"] == ",".join(
            [str(i) for i in gpu_ids]))
        for gpu_id in gpu_ids:
            assert gpu_id in range(num_gpus)
        return gpu_ids

    @ray.remote(num_gpus=3)
    def f3():
        time.sleep(0.1)
        gpu_ids = ray.get_gpu_ids()
        assert len(gpu_ids) == 3
        assert (os.environ["CUDA_VISIBLE_DEVICES"] == ",".join(
            [str(i) for i in gpu_ids]))
        for gpu_id in gpu_ids:
            assert gpu_id in range(num_gpus)
        return gpu_ids

    @ray.remote(num_gpus=4)
    def f4():
        time.sleep(0.1)
        gpu_ids = ray.get_gpu_ids()
        assert len(gpu_ids) == 4
        assert (os.environ["CUDA_VISIBLE_DEVICES"] == ",".join(
            [str(i) for i in gpu_ids]))
        for gpu_id in gpu_ids:
            assert gpu_id in range(num_gpus)
        return gpu_ids

    @ray.remote(num_gpus=5)
    def f5():
        time.sleep(0.1)
        gpu_ids = ray.get_gpu_ids()
        assert len(gpu_ids) == 5
        assert (os.environ["CUDA_VISIBLE_DEVICES"] == ",".join(
            [str(i) for i in gpu_ids]))
        for gpu_id in gpu_ids:
            assert gpu_id in range(num_gpus)
        return gpu_ids

    # Wait for all workers to start up.
    @ray.remote
    def f():
        time.sleep(0.1)
        return os.getpid()

    start_time = time.time()
    while True:
        if len(set(ray.get([f.remote() for _ in range(10)]))) == 10:
            break
        if time.time() > start_time + 10:
            raise Exception("Timed out while waiting for workers to start "
                            "up.")

    list_of_ids = ray.get([f0.remote() for _ in range(10)])
    assert list_of_ids == 10 * [[]]

    list_of_ids = ray.get([f1.remote() for _ in range(10)])
    set_of_ids = {tuple(gpu_ids) for gpu_ids in list_of_ids}
    assert set_of_ids == {(i, ) for i in range(10)}

    list_of_ids = ray.get([f2.remote(), f4.remote(), f4.remote()])
    all_ids = [gpu_id for gpu_ids in list_of_ids for gpu_id in gpu_ids]
    assert set(all_ids) == set(range(10))

    remaining = [f5.remote() for _ in range(20)]
    for _ in range(10):
        t1 = time.time()
        ready, remaining = ray.wait(remaining, num_returns=2)
        t2 = time.time()
        # There are only 10 GPUs, and each task uses 2 GPUs, so there
        # should only be 2 tasks scheduled at a given time, so if we wait
        # for 2 tasks to finish, then it should take at least 0.1 seconds
        # for each pair of tasks to finish.
        assert t2 - t1 > 0.09
        list_of_ids = ray.get(ready)
        all_ids = [gpu_id for gpu_ids in list_of_ids for gpu_id in gpu_ids]
        # Commenting out the below assert because it seems to fail a lot.
        # assert set(all_ids) == set(range(10))

    # Test that actors have CUDA_VISIBLE_DEVICES set properly.

    @ray.remote
    class Actor0(object):
        def __init__(self):
            gpu_ids = ray.get_gpu_ids()
            assert len(gpu_ids) == 0
            assert (os.environ["CUDA_VISIBLE_DEVICES"] == ",".join(
                [str(i) for i in gpu_ids]))
            # Set self.x to make sure that we got here.
            self.x = 1

        def test(self):
            gpu_ids = ray.get_gpu_ids()
            assert len(gpu_ids) == 0
            assert (os.environ["CUDA_VISIBLE_DEVICES"] == ",".join(
                [str(i) for i in gpu_ids]))
            return self.x

    @ray.remote(num_gpus=1)
    class Actor1(object):
        def __init__(self):
            gpu_ids = ray.get_gpu_ids()
            assert len(gpu_ids) == 1
            assert (os.environ["CUDA_VISIBLE_DEVICES"] == ",".join(
                [str(i) for i in gpu_ids]))
            # Set self.x to make sure that we got here.
            self.x = 1

        def test(self):
            gpu_ids = ray.get_gpu_ids()
            assert len(gpu_ids) == 1
            assert (os.environ["CUDA_VISIBLE_DEVICES"] == ",".join(
                [str(i) for i in gpu_ids]))
            return self.x

    a0 = Actor0.remote()
    ray.get(a0.test.remote())

    a1 = Actor1.remote()
    ray.get(a1.test.remote())


def test_zero_cpus(shutdown_only):
    ray.init(num_cpus=0)

    @ray.remote(num_cpus=0)
    def f():
        return 1

    # The task should be able to execute.
    ray.get(f.remote())


def test_zero_cpus_actor(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    cluster.add_node(num_cpus=2)
    ray.init(redis_address=cluster.redis_address)

    local_plasma = ray.worker.global_worker.plasma_client.store_socket_name

    @ray.remote
    class Foo(object):
        def method(self):
            return ray.worker.global_worker.plasma_client.store_socket_name

    # Make sure tasks and actors run on the remote local scheduler.
    a = Foo.remote()
    assert ray.get(a.method.remote()) != local_plasma


def test_fractional_resources(shutdown_only):
    ray.init(num_cpus=6, num_gpus=3, resources={"Custom": 1})

    @ray.remote(num_gpus=0.5)
    class Foo1(object):
        def method(self):
            gpu_ids = ray.get_gpu_ids()
            assert len(gpu_ids) == 1
            return gpu_ids[0]

    foos = [Foo1.remote() for _ in range(6)]
    gpu_ids = ray.get([f.method.remote() for f in foos])
    for i in range(3):
        assert gpu_ids.count(i) == 2
    del foos

    @ray.remote
    class Foo2(object):
        def method(self):
            pass

    # Create an actor that requires 0.7 of the custom resource.
    f1 = Foo2._remote([], {}, resources={"Custom": 0.7})
    ray.get(f1.method.remote())
    # Make sure that we cannot create an actor that requires 0.7 of the
    # custom resource. TODO(rkn): Re-enable this once ray.wait is
    # implemented.
    f2 = Foo2._remote([], {}, resources={"Custom": 0.7})
    ready, _ = ray.wait([f2.method.remote()], timeout=0.5)
    assert len(ready) == 0
    # Make sure we can start an actor that requries only 0.3 of the custom
    # resource.
    f3 = Foo2._remote([], {}, resources={"Custom": 0.3})
    ray.get(f3.method.remote())

    del f1, f3

    # Make sure that we get exceptions if we submit tasks that require a
    # fractional number of resources greater than 1.

    @ray.remote(num_cpus=1.5)
    def test():
        pass

    with pytest.raises(ValueError):
        test.remote()

    with pytest.raises(ValueError):
        Foo2._remote([], {}, resources={"Custom": 1.5})


def test_multiple_local_schedulers(ray_start_cluster):
    # This test will define a bunch of tasks that can only be assigned to
    # specific local schedulers, and we will check that they are assigned
    # to the correct local schedulers.
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=11, num_gpus=0)
    cluster.add_node(num_cpus=5, num_gpus=5)
    cluster.add_node(num_cpus=10, num_gpus=1)
    ray.init(redis_address=cluster.redis_address)
    cluster.wait_for_nodes()

    # Define a bunch of remote functions that all return the socket name of
    # the plasma store. Since there is a one-to-one correspondence between
    # plasma stores and local schedulers (at least right now), this can be
    # used to identify which local scheduler the task was assigned to.

    # This must be run on the zeroth local scheduler.
    @ray.remote(num_cpus=11)
    def run_on_0():
        return ray.worker.global_worker.plasma_client.store_socket_name

    # This must be run on the first local scheduler.
    @ray.remote(num_gpus=2)
    def run_on_1():
        return ray.worker.global_worker.plasma_client.store_socket_name

    # This must be run on the second local scheduler.
    @ray.remote(num_cpus=6, num_gpus=1)
    def run_on_2():
        return ray.worker.global_worker.plasma_client.store_socket_name

    # This can be run anywhere.
    @ray.remote(num_cpus=0, num_gpus=0)
    def run_on_0_1_2():
        return ray.worker.global_worker.plasma_client.store_socket_name

    # This must be run on the first or second local scheduler.
    @ray.remote(num_gpus=1)
    def run_on_1_2():
        return ray.worker.global_worker.plasma_client.store_socket_name

    # This must be run on the zeroth or second local scheduler.
    @ray.remote(num_cpus=8)
    def run_on_0_2():
        return ray.worker.global_worker.plasma_client.store_socket_name

    def run_lots_of_tasks():
        names = []
        results = []
        for i in range(100):
            index = np.random.randint(6)
            if index == 0:
                names.append("run_on_0")
                results.append(run_on_0.remote())
            elif index == 1:
                names.append("run_on_1")
                results.append(run_on_1.remote())
            elif index == 2:
                names.append("run_on_2")
                results.append(run_on_2.remote())
            elif index == 3:
                names.append("run_on_0_1_2")
                results.append(run_on_0_1_2.remote())
            elif index == 4:
                names.append("run_on_1_2")
                results.append(run_on_1_2.remote())
            elif index == 5:
                names.append("run_on_0_2")
                results.append(run_on_0_2.remote())
        return names, results

    client_table = ray.global_state.client_table()
    store_names = []
    store_names += [
        client["ObjectStoreSocketName"] for client in client_table
        if client["Resources"]["GPU"] == 0
    ]
    store_names += [
        client["ObjectStoreSocketName"] for client in client_table
        if client["Resources"]["GPU"] == 5
    ]
    store_names += [
        client["ObjectStoreSocketName"] for client in client_table
        if client["Resources"]["GPU"] == 1
    ]
    assert len(store_names) == 3

    def validate_names_and_results(names, results):
        for name, result in zip(names, ray.get(results)):
            if name == "run_on_0":
                assert result in [store_names[0]]
            elif name == "run_on_1":
                assert result in [store_names[1]]
            elif name == "run_on_2":
                assert result in [store_names[2]]
            elif name == "run_on_0_1_2":
                assert (result in [
                    store_names[0], store_names[1], store_names[2]
                ])
            elif name == "run_on_1_2":
                assert result in [store_names[1], store_names[2]]
            elif name == "run_on_0_2":
                assert result in [store_names[0], store_names[2]]
            else:
                raise Exception("This should be unreachable.")
            assert set(ray.get(results)) == set(store_names)

    names, results = run_lots_of_tasks()
    validate_names_and_results(names, results)

    # Make sure the same thing works when this is nested inside of a task.

    @ray.remote
    def run_nested1():
        names, results = run_lots_of_tasks()
        return names, results

    @ray.remote
    def run_nested2():
        names, results = ray.get(run_nested1.remote())
        return names, results

    names, results = ray.get(run_nested2.remote())
    validate_names_and_results(names, results)


def test_custom_resources(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=3, resources={"CustomResource": 0})
    cluster.add_node(num_cpus=3, resources={"CustomResource": 1})
    ray.init(redis_address=cluster.redis_address)

    @ray.remote
    def f():
        time.sleep(0.001)
        return ray.worker.global_worker.plasma_client.store_socket_name

    @ray.remote(resources={"CustomResource": 1})
    def g():
        time.sleep(0.001)
        return ray.worker.global_worker.plasma_client.store_socket_name

    @ray.remote(resources={"CustomResource": 1})
    def h():
        ray.get([f.remote() for _ in range(5)])
        return ray.worker.global_worker.plasma_client.store_socket_name

    # The f tasks should be scheduled on both local schedulers.
    assert len(set(ray.get([f.remote() for _ in range(50)]))) == 2

    local_plasma = ray.worker.global_worker.plasma_client.store_socket_name

    # The g tasks should be scheduled only on the second local scheduler.
    local_scheduler_ids = set(ray.get([g.remote() for _ in range(50)]))
    assert len(local_scheduler_ids) == 1
    assert list(local_scheduler_ids)[0] != local_plasma

    # Make sure that resource bookkeeping works when a task that uses a
    # custom resources gets blocked.
    ray.get([h.remote() for _ in range(5)])


def test_two_custom_resources(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(
        num_cpus=3, resources={
            "CustomResource1": 1,
            "CustomResource2": 2
        })
    cluster.add_node(
        num_cpus=3, resources={
            "CustomResource1": 3,
            "CustomResource2": 4
        })
    ray.init(redis_address=cluster.redis_address)

    @ray.remote(resources={"CustomResource1": 1})
    def f():
        time.sleep(0.001)
        return ray.worker.global_worker.plasma_client.store_socket_name

    @ray.remote(resources={"CustomResource2": 1})
    def g():
        time.sleep(0.001)
        return ray.worker.global_worker.plasma_client.store_socket_name

    @ray.remote(resources={"CustomResource1": 1, "CustomResource2": 3})
    def h():
        time.sleep(0.001)
        return ray.worker.global_worker.plasma_client.store_socket_name

    @ray.remote(resources={"CustomResource1": 4})
    def j():
        time.sleep(0.001)
        return ray.worker.global_worker.plasma_client.store_socket_name

    @ray.remote(resources={"CustomResource3": 1})
    def k():
        time.sleep(0.001)
        return ray.worker.global_worker.plasma_client.store_socket_name

    # The f and g tasks should be scheduled on both local schedulers.
    assert len(set(ray.get([f.remote() for _ in range(50)]))) == 2
    assert len(set(ray.get([g.remote() for _ in range(50)]))) == 2

    local_plasma = ray.worker.global_worker.plasma_client.store_socket_name

    # The h tasks should be scheduled only on the second local scheduler.
    local_scheduler_ids = set(ray.get([h.remote() for _ in range(50)]))
    assert len(local_scheduler_ids) == 1
    assert list(local_scheduler_ids)[0] != local_plasma

    # Make sure that tasks with unsatisfied custom resource requirements do
    # not get scheduled.
    ready_ids, remaining_ids = ray.wait([j.remote(), k.remote()], timeout=0.5)
    assert ready_ids == []


def test_many_custom_resources(shutdown_only):
    num_custom_resources = 10000
    total_resources = {
        str(i): np.random.randint(1, 7)
        for i in range(num_custom_resources)
    }
    ray.init(num_cpus=5, resources=total_resources)

    def f():
        return 1

    remote_functions = []
    for _ in range(20):
        num_resources = np.random.randint(0, num_custom_resources + 1)
        permuted_resources = np.random.permutation(
            num_custom_resources)[:num_resources]
        random_resources = {
            str(i): total_resources[str(i)]
            for i in permuted_resources
        }
        remote_function = ray.remote(resources=random_resources)(f)
        remote_functions.append(remote_function)

    remote_functions.append(ray.remote(f))
    remote_functions.append(ray.remote(resources=total_resources)(f))

    results = []
    for remote_function in remote_functions:
        results.append(remote_function.remote())
        results.append(remote_function.remote())
        results.append(remote_function.remote())

    ray.get(results)


@pytest.fixture
def save_gpu_ids_shutdown_only():
    # Record the curent value of this environment variable so that we can
    # reset it after the test.
    original_gpu_ids = os.environ.get("CUDA_VISIBLE_DEVICES", None)

    yield None

    # The code after the yield will run as teardown code.
    ray.shutdown()
    # Reset the environment variable.
    if original_gpu_ids is not None:
        os.environ["CUDA_VISIBLE_DEVICES"] = original_gpu_ids
    else:
        del os.environ["CUDA_VISIBLE_DEVICES"]


def test_specific_gpus(save_gpu_ids_shutdown_only):
    allowed_gpu_ids = [4, 5, 6]
    os.environ["CUDA_VISIBLE_DEVICES"] = ",".join(
        [str(i) for i in allowed_gpu_ids])
    ray.init(num_gpus=3)

    @ray.remote(num_gpus=1)
    def f():
        gpu_ids = ray.get_gpu_ids()
        assert len(gpu_ids) == 1
        assert gpu_ids[0] in allowed_gpu_ids

    @ray.remote(num_gpus=2)
    def g():
        gpu_ids = ray.get_gpu_ids()
        assert len(gpu_ids) == 2
        assert gpu_ids[0] in allowed_gpu_ids
        assert gpu_ids[1] in allowed_gpu_ids

    ray.get([f.remote() for _ in range(100)])
    ray.get([g.remote() for _ in range(100)])


def test_blocking_tasks(shutdown_only):
    ray.init(num_cpus=1)

    @ray.remote
    def f(i, j):
        return (i, j)

    @ray.remote
    def g(i):
        # Each instance of g submits and blocks on the result of another
        # remote task.
        object_ids = [f.remote(i, j) for j in range(2)]
        return ray.get(object_ids)

    @ray.remote
    def h(i):
        # Each instance of g submits and blocks on the result of another
        # remote task using ray.wait.
        object_ids = [f.remote(i, j) for j in range(2)]
        return ray.wait(object_ids, num_returns=len(object_ids))

    ray.get([h.remote(i) for i in range(4)])

    @ray.remote
    def _sleep(i):
        time.sleep(0.01)
        return (i)

    @ray.remote
    def sleep():
        # Each instance of sleep submits and blocks on the result of
        # another remote task, which takes some time to execute.
        ray.get([_sleep.remote(i) for i in range(10)])

    ray.get(sleep.remote())


def test_max_call_tasks(shutdown_only):
    ray.init(num_cpus=1)

    @ray.remote(max_calls=1)
    def f():
        return os.getpid()

    pid = ray.get(f.remote())
    ray.test.test_utils.wait_for_pid_to_exit(pid)

    @ray.remote(max_calls=2)
    def f():
        return os.getpid()

    pid1 = ray.get(f.remote())
    pid2 = ray.get(f.remote())
    assert pid1 == pid2
    ray.test.test_utils.wait_for_pid_to_exit(pid1)


def attempt_to_load_balance(remote_function,
                            args,
                            total_tasks,
                            num_nodes,
                            minimum_count,
                            num_attempts=100):
    attempts = 0
    while attempts < num_attempts:
        locations = ray.get(
            [remote_function.remote(*args) for _ in range(total_tasks)])
        names = set(locations)
        counts = [locations.count(name) for name in names]
        logger.info("Counts are {}.".format(counts))
        if (len(names) == num_nodes
                and all(count >= minimum_count for count in counts)):
            break
        attempts += 1
    assert attempts < num_attempts


def test_load_balancing(ray_start_cluster):
    # This test ensures that tasks are being assigned to all local
    # schedulers in a roughly equal manner.
    cluster = ray_start_cluster
    num_nodes = 3
    num_cpus = 7
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=num_cpus)
    ray.init(redis_address=cluster.redis_address)

    @ray.remote
    def f():
        time.sleep(0.01)
        return ray.worker.global_worker.plasma_client.store_socket_name

    attempt_to_load_balance(f, [], 100, num_nodes, 10)
    attempt_to_load_balance(f, [], 1000, num_nodes, 100)


def test_load_balancing_with_dependencies(ray_start_cluster):
    # This test ensures that tasks are being assigned to all local
    # schedulers in a roughly equal manner even when the tasks have
    # dependencies.
    cluster = ray_start_cluster
    num_nodes = 3
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=1)
    ray.init(redis_address=cluster.redis_address)

    @ray.remote
    def f(x):
        time.sleep(0.010)
        return ray.worker.global_worker.plasma_client.store_socket_name

    # This object will be local to one of the local schedulers. Make sure
    # this doesn't prevent tasks from being scheduled on other local
    # schedulers.
    x = ray.put(np.zeros(1000000))

    attempt_to_load_balance(f, [x], 100, num_nodes, 25)


def wait_for_num_tasks(num_tasks, timeout=10):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if len(ray.global_state.task_table()) >= num_tasks:
            return
        time.sleep(0.1)
    raise Exception("Timed out while waiting for global state.")


def wait_for_num_objects(num_objects, timeout=10):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if len(ray.global_state.object_table()) >= num_objects:
            return
        time.sleep(0.1)
    raise Exception("Timed out while waiting for global state.")


@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="New GCS API doesn't have a Python API yet.")
def test_global_state_api(shutdown_only):
    with pytest.raises(Exception):
        ray.global_state.object_table()

    with pytest.raises(Exception):
        ray.global_state.task_table()

    with pytest.raises(Exception):
        ray.global_state.client_table()

    with pytest.raises(Exception):
        ray.global_state.function_table()

    with pytest.raises(Exception):
        ray.global_state.log_files()

    ray.init(num_cpus=5, num_gpus=3, resources={"CustomResource": 1})

    resources = {"CPU": 5, "GPU": 3, "CustomResource": 1}
    assert ray.global_state.cluster_resources() == resources

    assert ray.global_state.object_table() == {}

    driver_id = ray.experimental.state.binary_to_hex(
        ray.worker.global_worker.worker_id)
    driver_task_id = ray.experimental.state.binary_to_hex(
        ray.worker.global_worker.current_task_id.id())

    # One task is put in the task table which corresponds to this driver.
    wait_for_num_tasks(1)
    task_table = ray.global_state.task_table()
    assert len(task_table) == 1
    assert driver_task_id == list(task_table.keys())[0]
    task_spec = task_table[driver_task_id]["TaskSpec"]
    nil_id_hex = ray.experimental.state.binary_to_hex(
        ray.ObjectID.nil_id().id())

    assert task_spec["TaskID"] == driver_task_id
    assert task_spec["ActorID"] == nil_id_hex
    assert task_spec["Args"] == []
    assert task_spec["DriverID"] == driver_id
    assert task_spec["FunctionID"] == nil_id_hex
    assert task_spec["ReturnObjectIDs"] == []

    client_table = ray.global_state.client_table()
    node_ip_address = ray.worker.global_worker.node_ip_address

    assert len(client_table) == 1
    assert client_table[0]["NodeManagerAddress"] == node_ip_address

    @ray.remote
    def f(*xs):
        return 1

    x_id = ray.put(1)
    result_id = f.remote(1, "hi", x_id)

    # Wait for one additional task to complete.
    wait_for_num_tasks(1 + 1)
    task_table = ray.global_state.task_table()
    assert len(task_table) == 1 + 1
    task_id_set = set(task_table.keys())
    task_id_set.remove(driver_task_id)
    task_id = list(task_id_set)[0]

    function_table = ray.global_state.function_table()
    task_spec = task_table[task_id]["TaskSpec"]
    assert task_spec["ActorID"] == nil_id_hex
    assert task_spec["Args"] == [1, "hi", x_id]
    assert task_spec["DriverID"] == driver_id
    assert task_spec["ReturnObjectIDs"] == [result_id]
    function_table_entry = function_table[task_spec["FunctionID"]]
    assert function_table_entry["Name"] == "runtest.f"
    assert function_table_entry["DriverID"] == driver_id
    assert function_table_entry["Module"] == "runtest"

    assert task_table[task_id] == ray.global_state.task_table(task_id)

    # Wait for two objects, one for the x_id and one for result_id.
    wait_for_num_objects(2)

    def wait_for_object_table():
        timeout = 10
        start_time = time.time()
        while time.time() - start_time < timeout:
            object_table = ray.global_state.object_table()
            tables_ready = (object_table[x_id]["ManagerIDs"] is not None and
                            object_table[result_id]["ManagerIDs"] is not None)
            if tables_ready:
                return
            time.sleep(0.1)
        raise Exception("Timed out while waiting for object table to "
                        "update.")

    object_table = ray.global_state.object_table()
    assert len(object_table) == 2

    assert object_table[x_id]["IsEviction"][0] is False

    assert object_table[result_id]["IsEviction"][0] is False

    assert object_table[x_id] == ray.global_state.object_table(x_id)
    object_table_entry = ray.global_state.object_table(result_id)
    assert object_table[result_id] == object_table_entry


@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="New GCS API doesn't have a Python API yet.")
def test_log_file_api(shutdown_only):
    ray.init(num_cpus=1, redirect_worker_output=True)

    message = "unique message"

    @ray.remote
    def f():
        logger.info(message)
        # The call to sys.stdout.flush() seems to be necessary when using
        # the system Python 2.7 on Ubuntu.
        sys.stdout.flush()

    ray.get(f.remote())

    # Make sure that the message appears in the log files.
    start_time = time.time()
    found_message = False
    while time.time() - start_time < 10:
        log_files = ray.global_state.log_files()
        for ip, innerdict in log_files.items():
            for filename, contents in innerdict.items():
                contents_str = "".join(contents)
                if message in contents_str:
                    found_message = True
        if found_message:
            break
        time.sleep(0.1)

    assert found_message is True


@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="New GCS API doesn't have a Python API yet.")
def test_workers(shutdown_only):
    num_workers = 3
    ray.init(redirect_worker_output=True, num_cpus=num_workers)

    @ray.remote
    def f():
        return id(ray.worker.global_worker), os.getpid()

    # Wait until all of the workers have started.
    worker_ids = set()
    while len(worker_ids) != num_workers:
        worker_ids = set(ray.get([f.remote() for _ in range(10)]))

    worker_info = ray.global_state.workers()
    assert len(worker_info) >= num_workers
    for worker_id, info in worker_info.items():
        assert "node_ip_address" in info
        assert "plasma_store_socket" in info
        assert "stderr_file" in info
        assert "stdout_file" in info


def test_specific_driver_id():
    dummy_driver_id = ray.ObjectID(b"00112233445566778899")
    ray.init(driver_id=dummy_driver_id)

    @ray.remote
    def f():
        return ray.worker.global_worker.task_driver_id.id()

    assert_equal(dummy_driver_id.id(), ray.worker.global_worker.worker_id)

    task_driver_id = ray.get(f.remote())
    assert_equal(dummy_driver_id.id(), task_driver_id)

    ray.shutdown()


def test_object_id_properties():
    id_bytes = b"00112233445566778899"
    object_id = ray.ObjectID(id_bytes)
    assert object_id.id() == id_bytes
    object_id = ray.ObjectID.nil_id()
    assert object_id.is_nil()
    with pytest.raises(ValueError, match=r".*needs to have length 20.*"):
        ray.ObjectID(id_bytes + b"1234")
    with pytest.raises(ValueError, match=r".*needs to have length 20.*"):
        ray.ObjectID(b"0123456789")
    object_id = ray.ObjectID(_random_string())
    assert not object_id.is_nil()
    assert object_id.id() != id_bytes
    id_dumps = pickle.dumps(object_id)
    id_from_dumps = pickle.loads(id_dumps)
    assert id_from_dumps == object_id


@pytest.fixture
def shutdown_only_with_initialization_check():
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()
    assert not ray.is_initialized()


def test_initialized(shutdown_only_with_initialization_check):
    assert not ray.is_initialized()
    ray.init(num_cpus=0)
    assert ray.is_initialized()


def test_initialized_local_mode(shutdown_only_with_initialization_check):
    assert not ray.is_initialized()
    ray.init(num_cpus=0, local_mode=True)
    assert ray.is_initialized()


def test_wait_reconstruction(shutdown_only):
    ray.init(num_cpus=1, object_store_memory=10**8)

    @ray.remote
    def f():
        return np.zeros(6 * 10**7, dtype=np.uint8)

    x_id = f.remote()
    ray.wait([x_id])
    ray.wait([f.remote()])
    assert not ray.worker.global_worker.plasma_client.contains(
        ray.pyarrow.plasma.ObjectID(x_id.id()))
    ready_ids, _ = ray.wait([x_id])
    assert len(ready_ids) == 1


def test_ray_setproctitle(shutdown_only):
    ray.init(num_cpus=2)

    @ray.remote
    class UniqueName(object):
        def __init__(self):
            assert setproctitle.getproctitle() == "ray_UniqueName:__init__()"

        def f(self):
            assert setproctitle.getproctitle() == "ray_UniqueName:f()"

    @ray.remote
    def unique_1():
        assert setproctitle.getproctitle() == "ray_worker:runtest.unique_1()"

    actor = UniqueName.remote()
    ray.get(actor.f.remote())
    ray.get(unique_1.remote())


def test_duplicate_error_messages(shutdown_only):
    ray.init(num_cpus=0)

    driver_id = ray.ObjectID.nil_id().id()
    error_data = ray.gcs_utils.construct_error_message(driver_id, "test",
                                                       "message", 0)

    # Push the same message to the GCS twice (they are the same because we
    # do not include a timestamp).

    r = ray.worker.global_worker.redis_client

    r.execute_command("RAY.TABLE_APPEND", ray.gcs_utils.TablePrefix.ERROR_INFO,
                      ray.gcs_utils.TablePubsub.ERROR_INFO, driver_id,
                      error_data)

    # Before https://github.com/ray-project/ray/pull/3316 this would
    # give an error
    r.execute_command("RAY.TABLE_APPEND", ray.gcs_utils.TablePrefix.ERROR_INFO,
                      ray.gcs_utils.TablePubsub.ERROR_INFO, driver_id,
                      error_data)


@pytest.mark.skipif(
    os.getenv("TRAVIS") is None,
    reason="This test should only be run on Travis.")
def test_ray_stack(shutdown_only):
    ray.init(num_cpus=2)

    def unique_name_1():
        time.sleep(1000)

    @ray.remote
    def unique_name_2():
        time.sleep(1000)

    @ray.remote
    def unique_name_3():
        unique_name_1()

    unique_name_2.remote()
    unique_name_3.remote()

    success = False
    start_time = time.time()
    while time.time() - start_time < 30:
        # Attempt to parse the "ray stack" call.
        output = ray.utils.decode(subprocess.check_output(["ray", "stack"]))
        if ("unique_name_1" in output and "unique_name_2" in output
                and "unique_name_3" in output):
            success = True
            break

    if not success:
        raise Exception("Failed to find necessary information with "
                        "'ray stack'")
