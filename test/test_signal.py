import pytest
import time

import ray
import ray.experimental.signal as signal

class UserSignal(signal.Signal):
    def __init__(self, value):
        self.value = value


@pytest.fixture
def ray_start():
    # Start the Ray processes.
    ray.init(num_cpus=4)
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_task_to_driver(ray_start):
    # Send a signal from a task and another signal on behalf of the task
    # from the driver. The driver gets both signals.

    @ray.remote
    def task_send_signal(value):
        signal.send(UserSignal(value))
        return

    signal_value = "simple signal"
    object_id = task_send_signal.remote(signal_value)
    ray.get(object_id)
    result_list = signal.receive([object_id], timeout=10)
    assert len(result_list) == 2
    assert type(result_list[1][1]) == signal.DoneSignal


def test_send_signal_from_actor_to_driver(ray_start):
    # Send several signals from an actor, and receive them in the driver.

    @ray.remote
    class ActorSendSignal(object):
        def __init__(self):
            pass
        def send_signal(self, value):
            signal.send(UserSignal(value))

    a = ActorSendSignal.remote()
    signal_value = "simple signal"
    count = 6
    for i in range(count):
        ray.get(a.send_signal.remote(signal_value + str(i)))

    result_list = signal.receive([a], timeout=10)
    assert len(result_list) == count
    for i in range(count):
        assert signal_value + str(i) == result_list[i][1].value


def test_send_signals_from_actor_to_driver(ray_start):
    # Send "count" signal at intervals of 100ms from an actor and get
    # these signals in the driver.

    @ray.remote
    class ActorSendSignals(object):
        def __init__(self):
            pass
        def send_signals(self, value, count):
            for i in range(count):
                signal.send(UserSignal(value + str(i)))

    a = ActorSendSignals.remote()
    signal_value = "simple signal"
    count = 20
    a.send_signals.remote(signal_value, count)
    received_count = 0
    while True:
        result_list = signal.receive([a], timeout=1)
        received_count += len(result_list)
        if (received_count == count):
            break
    assert True


def test_task_done(ray_start):

    @ray.remote
    def test_function():
        return 1

    object_id = test_function.remote()
    ray.get(object_id)
    result_list = signal.receive([object_id], timeout=5)
    assert len(result_list) == 1
    assert type(result_list[0][1]) == signal.DoneSignal


def test_task_crash(ray_start):
    # Get an error when ray.get() is called on the return of a failed task.

    @ray.remote
    def crashing_function():
        raise Exception("exception message")

    object_id = crashing_function.remote()
    try:
        ray.get(object_id)
    except Exception as e:
        assert type(e) == ray.worker.RayTaskError
    finally:
        result_list = signal.receive([object_id], timeout=5)
        assert len(result_list) == 1
        assert type(result_list[0][1]) == signal.ErrorSignal


def test_task_crash_without_get(ray_start):
    # Get an error when task failed.

    @ray.remote
    def crashing_function():
        raise Exception("exception message")

    object_id = crashing_function.remote()
    result_list = signal.receive([object_id], timeout=5)
    assert len(result_list) == 1
    assert type(result_list[0][1]) == signal.ErrorSignal


def test_actor_crash(ray_start):
    # Get an error when ray.get() is called on a return parameter
    # of a method that filed.

    @ray.remote
    class Actor(object):
        def __init__(self):
            pass
        def crash(self):
            raise Exception("exception message")

    a = Actor.remote()
    try:
        ray.get(a.crash.remote())
    except Exception as e:
        assert type(e) == ray.worker.RayTaskError
    finally:
        result_list = signal.receive([a], timeout=5)
        assert len(result_list) == 1
        assert type(result_list[0][1]) == signal.ErrorSignal


def test_actor_crash_init(ray_start):
    # Get an error when an actor's __init__ failed.

    @ray.remote
    class ActorCrashInit(object):
        def __init__(self):
            raise Exception("exception message")
        def m(self):
            return 1

    # Using try and catch won't catch the exception in the __init__.
    a = ActorCrashInit.remote()
    time.sleep(2)
    result_list = signal.receive([a], timeout = 5)
    assert len(result_list) == 1
    assert type(result_list[0][1]) == signal.ErrorSignal


def test_actor_crash_init2(ray_start):
    # Get errors when (1) __init__ fails, and (2) subsequently when ray.get() is
    # called on the return parameter of another method of the actor.

    @ray.remote
    class ActorCrashInit(object):
        def __init__(self):
            raise Exception("exception message")
        def method(self):
            return 1

    a = ActorCrashInit.remote()
    try:
        ray.get(a.method.remote())
    except Exception as e:
        assert type(e) == ray.worker.RayTaskError
    finally:
        # TODO: this needs to be removed
        time.sleep(5)
        result_list = signal.receive([a], timeout=5)
        assert len(result_list) == 2
        assert type(result_list[0][1]) == signal.ErrorSignal


def test_actor_crash_init3(ray_start):
    # Get errors when (1) __init__ fails, and (2) subsequently when
    # another method of the actor is invoked.

    @ray.remote
    class ActorCrashInit(object):
        def __init__(self):
            raise Exception("exception message")
        def method(self):
            return 1

    a = ActorCrashInit.remote()
    a.method.remote()
    time.sleep(2)
    result_list = signal.receive([a], timeout=5)
    assert len(result_list) == 2
    assert type(result_list[0][1]) == signal.ErrorSignal


def test_send_signals_from_actor_to_actor(ray_start):
    # Send "count" signal at intervals of 100ms from two actors and get
    # these signals in another actor.

    @ray.remote
    class ActorSendSignals(object):
        def __init__(self):
            pass
        def send_signals(self, value, count):
            for i in range(count):
                signal.send(UserSignal(value + str(i)))

    @ray.remote
    class ActorGetSignalsAll(object):
        def __init__(self):
            self.received_signals = []
        def register_handle(self, handle):
            self.this_actor = handle
        def get_signals(self, source_ids, count):
            new_signals = signal.receive(source_ids, timeout=10)
            for s in new_signals:
                self.received_signals.append(s)
            if len(self.received_signals) < count:
                self.this_actor.get_signals.remote(source_ids, count)
            else:
                return
        def get_count(self):
            return len(self.received_signals)

    a1 = ActorSendSignals.remote()
    a2 = ActorSendSignals.remote()
    signal_value = "simple signal"
    count = 20
    ray.get(a1.send_signals.remote(signal_value, count))
    ray.get(a2.send_signals.remote(signal_value, count))

    b = ActorGetSignalsAll.remote()
    ray.get(b.register_handle.remote(b))
    b.get_signals.remote([a1, a2], count)
    received_count = ray.get(b.get_count.remote())
    assert received_count == 2 * count


def test_forget(ray_start):
    # Send "count" signals on behalf of an actor, then ignore all these signals,
    # and then send anther "count" signals on behalf of the same actor. Then
    # show that the driver only gets the last "count" signals.

    @ray.remote
    class ActorSendSignals(object):
        def __init__(self):
            pass
        def send_signals(self, value, count):
            for i in range(count):
                signal.send(UserSignal(value + str(i)))

    a = ActorSendSignals.remote()
    signal_value = "simple signal"
    count = 5
    ray.get(a.send_signals.remote(signal_value, count))
    signal.forget([a])
    ray.get(a.send_signals.remote(signal_value, count))
    result_list = signal.receive([a], timeout=10)
    assert len(result_list) == count
    # for i in range(count):
    #    assert signal_value + str(i) == (result_list[i][1]).value()
