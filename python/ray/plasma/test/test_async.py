from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import asyncio
from collections import namedtuple
import math
import unittest
import random
import ray
from ray.experimental.plasma_eventloop import PlasmaPoll, PlasmaEpoll, \
    PlasmaSelectorEventLoop

HashFlowNode = namedtuple('HashFlowNode', ['parents', 'delay', 'result'])


class PlasmaEventLoopUsePoll(PlasmaSelectorEventLoop):
    def __init__(self):
        self.selector = PlasmaPoll(ray.worker.global_worker)
        super().__init__(self.selector, worker=ray.worker.global_worker)

    def __enter__(self):
        self.set_debug(False)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class PlasmaEventLoopUseEpoll(PlasmaSelectorEventLoop):
    def __init__(self):
        self.selector = PlasmaEpoll(ray.worker.global_worker)
        super().__init__(self.selector, worker=ray.worker.global_worker)

    def __enter__(self):
        self.set_debug(False)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def gen_hashflow(seed, width, depth):
    random.seed(seed)
    n = int(math.log2(width))
    inputs = [i.to_bytes(20, byteorder='big') for i in range(width)]
    stages = []
    for _ in range(depth):
        nodes = [
            HashFlowNode(
                parents=[random.randint(0, width - 1) for _ in range(n)],
                delay=random.random() * 0.1,
                result=None) for _ in range(width)
        ]
        stages.append(nodes)

    stages.append([
        HashFlowNode(
            parents=list(range(width)), delay=random.random(), result=None)
    ])

    return inputs, stages


def calc_hashflow(inputs, delay=None):
    import time
    import hashlib

    if delay is not None:
        time.sleep(delay)

    m = hashlib.sha256()

    for item in inputs:
        if isinstance(item, ray.local_scheduler.ObjectID):
            item = ray.get(item)
        m.update(item)

    return m.digest()


def default_hashflow_solution(inputs, stages, use_delay=False):
    # forced to re-register the function
    # because Ray may be re-init in another test
    calc_hashflow_remote = ray.remote(calc_hashflow)
    inputs = list(map(ray.put, inputs))
    for i, stage in enumerate(stages):
        new_inputs = []
        for node in stage:
            node_inputs = [inputs[i] for i in node.parents]
            delay = node.delay if use_delay else None
            new_inputs.append(
                calc_hashflow_remote.remote(node_inputs, delay=delay))
        inputs = new_inputs

    return ray.get(inputs[0])


def wait_and_solve(inputs, node, use_delay, loop):
    # forced to re-register the function
    # because Ray may be re-init in another test
    calc_hashflow_remote = ray.remote(calc_hashflow)

    @asyncio.coroutine
    def _wait_and_solve(a_inputs):
        r_inputs = yield from a_inputs
        delay = node.delay if use_delay else None
        return calc_hashflow_remote.remote(r_inputs, delay=delay)

    return asyncio.ensure_future(_wait_and_solve(inputs), loop=loop)


def async_hashflow_solution_get(inputs, stages, use_delay=False):
    with PlasmaEventLoopUseEpoll() as loop:
        inputs = list(map(ray.put, inputs))
        for i, stage in enumerate(stages):
            new_inputs = []
            for node in stage:
                node_inputs = [inputs[i] for i in node.parents]
                async_inputs = loop.get(node_inputs)
                new_inputs.append(
                    wait_and_solve(async_inputs, node, use_delay, loop=loop))
            inputs = new_inputs

        result = loop.run_until_complete(inputs[0])
    return ray.get(result)


def async_hashflow_solution_wait(inputs, stages, use_delay=False):
    @asyncio.coroutine
    def return_first_item(coro):
        result = yield from coro
        return result[0]

    with PlasmaEventLoopUseEpoll() as loop:
        inputs = list(map(ray.put, inputs))
        for i, stage in enumerate(stages):
            new_inputs = []
            for node in stage:
                node_inputs = [inputs[i] for i in node.parents]
                async_inputs = loop.wait(
                    node_inputs, num_returns=len(node_inputs))
                ready = return_first_item(async_inputs)
                new_inputs.append(
                    wait_and_solve(ready, node, use_delay, loop=loop))
            inputs = new_inputs
        result = loop.run_until_complete(inputs[0])
    return ray.get(result)


class TestAsyncPlasmaPollBasic(unittest.TestCase):
    def setUp(self):
        # Start the Ray processes.
        ray.init()

    def tearDown(self):
        ray.worker.cleanup()

    def test_get(self):
        @ray.remote
        def f(n):
            import time
            time.sleep(n)
            return n

        with PlasmaEventLoopUsePoll() as loop:
            tasks = [f.remote(i) for i in range(5)]
            results = loop.run_until_complete(loop.get(tasks))
        self.assertListEqual(results, ray.get(tasks))

    def test_wait(self):
        @ray.remote
        def f(n):
            import time
            time.sleep(n)
            return n

        with PlasmaEventLoopUsePoll() as loop:
            tasks = [f.remote(i) for i in range(5)]
            results, _ = loop.run_until_complete(
                loop.wait(tasks, num_returns=len(tasks)))
        self.assertEqual(set(results), set(tasks))

    def test_wait_timeout(self):
        @ray.remote
        def f(n):
            import time
            time.sleep(n * 20)
            return n

        with PlasmaEventLoopUsePoll() as loop:
            tasks = [f.remote(i) for i in range(5)]
            results, _ = loop.run_until_complete(
                loop.wait(tasks, timeout=10, num_returns=len(tasks)))
        self.assertEqual(results[0], tasks[0])


class TestAsyncPlasmaEpollBasic(unittest.TestCase):
    def setUp(self):
        # Start the Ray processes.
        ray.init()

    def tearDown(self):
        ray.worker.cleanup()

    def test_get(self):
        @ray.remote
        def f(n):
            import time
            time.sleep(n)
            return n

        with PlasmaEventLoopUseEpoll() as loop:
            tasks = [f.remote(i) for i in range(5)]
            results = loop.run_until_complete(loop.get(tasks))
        self.assertListEqual(results, ray.get(tasks))

    def test_wait(self):
        @ray.remote
        def f(n):
            import time
            time.sleep(n)
            return n

        with PlasmaEventLoopUseEpoll() as loop:
            tasks = [f.remote(i) for i in range(5)]
            results, _ = loop.run_until_complete(
                loop.wait(tasks, num_returns=len(tasks)))
        self.assertEqual(set(results), set(tasks))

    def test_wait_timeout(self):
        @ray.remote
        def f(n):
            import time
            time.sleep(n * 20)
            return n

        with PlasmaEventLoopUseEpoll() as loop:
            tasks = [f.remote(i) for i in range(5)]
            results, _ = loop.run_until_complete(
                loop.wait(tasks, timeout=10, num_returns=len(tasks)))
        self.assertEqual(results[0], tasks[0])


class TestAsyncPlasmaAPI(unittest.TestCase):
    def setUp(self):
        # Start the Ray processes.
        ray.init()

    def tearDown(self):
        ray.worker.cleanup()

    def test_get(self):
        @ray.remote
        def f(n):
            import time
            time.sleep(n)
            return n

        tasks = [f.remote(i) for i in range(5)]
        fut = ray.get(tasks, blocking=False)
        ray.worker.global_worker.eventloop.set_debug(True)
        results = ray.worker.run_until_complete(fut)
        self.assertListEqual(results, ray.get(tasks))

    def test_wait(self):
        @ray.remote
        def f(n):
            import time
            time.sleep(n)
            return n

        tasks = [f.remote(i) for i in range(5)]
        fut = ray.wait(tasks, num_returns=len(tasks), blocking=False)
        ray.worker.global_worker.eventloop.set_debug(True)
        results, _ = ray.worker.run_until_complete(fut)
        self.assertEqual(set(results), set(tasks))

    def test_wait_timeout(self):
        @ray.remote
        def f(n):
            import time
            time.sleep(n * 20)
            return n

        tasks = [f.remote(i) for i in range(5)]
        fut = ray.wait(
            tasks, timeout=10, num_returns=len(tasks), blocking=False)
        ray.worker.global_worker.eventloop.set_debug(True)
        results, _ = ray.worker.run_until_complete(fut)
        self.assertEqual(results[0], tasks[0])


class TestAsyncPlasma(unittest.TestCase):
    answer = b'U\x16\xc5c\x0fa\xdcx\x03\x1e\xf7\xd8&{\xece' \
             b'\x85-.O\x12\xed\x11[\xdc\xe6\xcc\xdf\x90\x91\xc7\xf7'

    def setUp(self):
        # Start the Ray processes.
        ray.init(num_cpus=2)

    def tearDown(self):
        ray.worker.cleanup()

    def test_baseline(self):
        inputs, stages = gen_hashflow(0, 16, 16)
        ground_truth = default_hashflow_solution(inputs, stages, True)
        self.assertEqual(ground_truth, self.answer)

    def test_async_get(self):
        inputs, stages = gen_hashflow(0, 16, 16)
        result = async_hashflow_solution_get(inputs, stages, use_delay=True)
        self.assertEqual(result, self.answer)

    def test_async_wait(self):
        inputs, stages = gen_hashflow(0, 16, 16)
        result = async_hashflow_solution_wait(inputs, stages, use_delay=True)
        self.assertEqual(result, self.answer)


if __name__ == "__main__":
    import sys

    assert sys.version_info > (3, 2)
    unittest.main(verbosity=2)
