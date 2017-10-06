from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import cloudpickle as pickle
import copy
import hashlib
import inspect
import json
import numpy as np
import traceback

import pyarrow.plasma as plasma
import ray.local_scheduler
import ray.signature as signature
import ray.worker
from ray.utils import (binary_to_hex, FunctionProperties, random_string,
                       release_gpus_in_use, select_local_scheduler)


def random_actor_id():
    return ray.local_scheduler.ObjectID(random_string())


def random_actor_class_id():
    return random_string()


def get_actor_method_function_id(attr):
    """Get the function ID corresponding to an actor method.

    Args:
        attr (str): The attribute name of the method.

    Returns:
        Function ID corresponding to the method.
    """
    function_id_hash = hashlib.sha1()
    function_id_hash.update(attr.encode("ascii"))
    function_id = function_id_hash.digest()
    assert len(function_id) == 20
    return ray.local_scheduler.ObjectID(function_id)


def get_checkpoint_indices(worker, actor_id):
    actor_key = b"Actor:" + actor_id
    checkpoint_indices = []
    for key in worker.redis_client.hkeys(actor_key):
        if key.startswith(b"checkpoint_"):
            index = int(key[len(b"checkpoint_"):])
            checkpoint_indices.append(index)
    return checkpoint_indices


def get_actor_checkpoint(worker, actor_id):
    """Get the most recent checkpoint associated with a given actor ID.

    Args:
        actor_id: The actor ID of the actor to get the checkpoint for.
        worker: The worker to use to get the checkpoint.

    Returns:
        If a checkpoint exists, this returns a tuple of the checkpoint index
            and the checkpoint. Otherwise it returns (-1, None). The checkpoint
            index is the actor counter of the last task that was executed on
            the actor before the checkpoint was made.
    """
    # Get all of the keys associated with checkpoints for this actor.
    checkpoint_indices = get_checkpoint_indices(worker, actor_id)
    if len(checkpoint_indices) == 0:
        return -1, None
    else:
        actor_key = b"Actor:" + actor_id
        checkpoint_index = max(checkpoint_indices)
        checkpoint = worker.redis_client.hget(
            actor_key, "checkpoint_{}".format(checkpoint_index))
        return checkpoint_index, checkpoint


def put_dummy_object(worker, dummy_return_id):
    # Add the dummy output for actor tasks. TODO(swang): We use
    # a numpy array as a hack to pin the object in the object
    # store.  Once we allow object pinning in the store, we may
    # use `None`.
    dummy_object = np.zeros(1)
    worker.put_object(dummy_return_id, dummy_object)
    # Keep the dummy output in scope for the lifetime of the
    # actor, to prevent eviction from the object store.
    dummy_object = worker.get_object([dummy_return_id])
    dummy_object = dummy_object[0]
    worker.actor_pinned_objects[dummy_return_id] = dummy_object


def make_actor_method_executor(worker, method_name, method):

    def actor_method_executor(dummy_return_id, task_counter, actor,
                              *args):
        if method_name == "__ray_checkpoint__":
            resumed = method(actor, *args)
            if resumed:
                put_dummy_object(worker, dummy_return_id)
                worker.actor_task_counter = task_counter + 1
            return None
        else:
            # If this is any actor task other than the first, which
            # has no dependencies, the last argument is a dummy
            # argument that represents the dependency on the
            # previous actor task. Remove this argument for
            # invocation.
            if worker.actor_task_counter > 0:
                args = args[:-1]
            put_dummy_object(worker, dummy_return_id)
            print("put dummy object for task", task_counter)
            worker.actor_task_counter = task_counter + 1
            return method(actor, *args)
    return actor_method_executor


def fetch_and_register_actor(actor_class_key, worker):
    """Import an actor.

    This will be called by the worker's import thread when the worker receives
    the actor_class export, assuming that the worker is an actor for that
    class.
    """
    actor_id_str = worker.actor_id
    (driver_id, class_id, class_name,
     module, pickled_class, checkpoint_interval,
     actor_method_names) = worker.redis_client.hmget(
         actor_class_key, ["driver_id", "class_id", "class_name", "module",
                           "class", "checkpoint_interval",
                           "actor_method_names"])

    actor_name = class_name.decode("ascii")
    module = module.decode("ascii")
    checkpoint_interval = int(checkpoint_interval)
    actor_method_names = json.loads(actor_method_names.decode("ascii"))

    # Create a temporary actor with some temporary methods so that if the actor
    # fails to be unpickled, the temporary actor can be used (just to produce
    # error messages and to prevent the driver from hanging).
    class TemporaryActor(object):
        pass
    worker.actors[actor_id_str] = TemporaryActor()
    worker.actor_checkpoint_interval = checkpoint_interval

    def temporary_actor_method(*xs):
        raise Exception("The actor with name {} failed to be imported, and so "
                        "cannot execute this method".format(actor_name))
    for actor_method_name in actor_method_names:
        function_id = get_actor_method_function_id(actor_method_name).id()
        worker.functions[driver_id][function_id] = (actor_method_name,
                                                    temporary_actor_method)
        worker.function_properties[driver_id][function_id] = (
            FunctionProperties(num_return_vals=2,
                               num_cpus=1,
                               num_gpus=0,
                               num_custom_resource=0,
                               max_calls=0))
        worker.num_task_executions[driver_id][function_id] = 0

    try:
        unpickled_class = pickle.loads(pickled_class)
    except Exception:
        # If an exception was thrown when the actor was imported, we record the
        # traceback and notify the scheduler of the failure.
        traceback_str = ray.worker.format_error_message(traceback.format_exc())
        # Log the error message.
        worker.push_error_to_driver(driver_id, "register_actor", traceback_str,
                                    data={"actor_id": actor_id_str})
        # TODO(rkn): In the future, it might make sense to have the worker exit
        # here. However, currently that would lead to hanging if someone calls
        # ray.get on a method invoked on the actor.
    else:
        # TODO(pcm): Why is the below line necessary?
        unpickled_class.__module__ = module
        worker.actors[actor_id_str] = unpickled_class.__new__(unpickled_class)
        actor_methods = inspect.getmembers(
            unpickled_class, predicate=(lambda x: (inspect.isfunction(x) or
                                                   inspect.ismethod(x))))
        for actor_method_name, actor_method in actor_methods:
            function_id = get_actor_method_function_id(actor_method_name).id()

            worker.functions[driver_id][function_id] = (
                actor_method_name,
                make_actor_method_executor(worker, actor_method_name,
                                           actor_method))
            # We do not set worker.function_properties[driver_id][function_id]
            # because we currently do need the actor worker to submit new tasks
            # for the actor.

        # Store some extra information that will be used when the actor exits
        # to release GPU resources.
        worker.driver_id = binary_to_hex(driver_id)
        local_scheduler_id = worker.redis_client.hget(
            b"Actor:" + actor_id_str, "local_scheduler_id")
        worker.local_scheduler_id = binary_to_hex(local_scheduler_id)


def export_actor_class(class_id, Class, actor_method_names,
                       checkpoint_interval, worker):
    if worker.mode is None:
        raise Exception("Actors cannot be created before Ray has been "
                        "started. You can start Ray with 'ray.init()'.")
    key = b"ActorClass:" + class_id
    d = {"driver_id": worker.task_driver_id.id(),
         "class_name": Class.__name__,
         "module": Class.__module__,
         "class": pickle.dumps(Class),
         "checkpoint_interval": checkpoint_interval,
         "actor_method_names": json.dumps(list(actor_method_names))}
    worker.redis_client.hmset(key, d)
    worker.redis_client.rpush("Exports", key)


def export_actor(actor_id, class_id, actor_method_names, num_cpus, num_gpus,
                 worker):
    """Export an actor to redis.

    Args:
        actor_id: The ID of the actor.
        actor_method_names (list): A list of the names of this actor's methods.
        num_cpus (int): The number of CPUs that this actor requires.
        num_gpus (int): The number of GPUs that this actor requires.
    """
    ray.worker.check_main_thread()
    if worker.mode is None:
        raise Exception("Actors cannot be created before Ray has been "
                        "started. You can start Ray with 'ray.init()'.")
    key = b"Actor:" + actor_id.id()

    # For now, all actor methods have 1 return value.
    driver_id = worker.task_driver_id.id()
    for actor_method_name in actor_method_names:
        # TODO(rkn): When we create a second actor, we are probably overwriting
        # the values from the first actor here. This may or may not be a
        # problem.
        function_id = get_actor_method_function_id(actor_method_name).id()
        worker.function_properties[driver_id][function_id] = (
            FunctionProperties(num_return_vals=2,
                               num_cpus=1,
                               num_gpus=0,
                               num_custom_resource=0,
                               max_calls=0))

    # Select a local scheduler for the actor.
    local_scheduler_id = select_local_scheduler(
        worker.task_driver_id.id(), ray.global_state.local_schedulers(),
        num_gpus, worker.redis_client)
    assert local_scheduler_id is not None

    # We must put the actor information in Redis before publishing the actor
    # notification so that when the newly created actor attempts to fetch the
    # information from Redis, it is already there.
    worker.redis_client.hmset(key, {"class_id": class_id,
                                    "driver_id": driver_id,
                                    "local_scheduler_id": local_scheduler_id,
                                    "num_gpus": num_gpus,
                                    "removed": False})

    # TODO(rkn): There is actually no guarantee that the local scheduler that
    # we are publishing to has already subscribed to the actor_notifications
    # channel. Therefore, this message may be missed and the workload will
    # hang. This is a bug.
    ray.utils.publish_actor_creation(actor_id.id(), driver_id,
                                     local_scheduler_id, False,
                                     worker.redis_client)


def make_actor(cls, num_cpus, num_gpus, checkpoint_interval):
    checkpoint_interval += 1

    # Modify the class to have an additional method that will be used for
    # terminating the worker.
    class Class(cls):
        def __ray_terminate__(self, actor_id):
            # Record that this actor has been removed so that if this node
            # dies later, the actor won't be recreated. Alternatively, we could
            # remove the actor key from Redis here.
            ray.worker.global_worker.redis_client.hset(b"Actor:" + actor_id,
                                                       "removed", True)
            # Release the GPUs that this worker was using.
            if len(ray.get_gpu_ids()) > 0:
                release_gpus_in_use(
                    ray.worker.global_worker.driver_id,
                    ray.worker.global_worker.local_scheduler_id,
                    ray.get_gpu_ids(),
                    ray.worker.global_worker.redis_client)
            # Disconnect the worker from the local scheduler. The point of this
            # is so that when the worker kills itself below, the local
            # scheduler won't push an error message to the driver.
            ray.worker.global_worker.local_scheduler_client.disconnect()
            import os
            os._exit(0)

        def __ray_save_checkpoint__(self):
            if hasattr(self, "__ray_save__"):
                object_to_serialize = self.__ray_save__()
            else:
                object_to_serialize = self
            return pickle.dumps(object_to_serialize)

        @classmethod
        def __ray_restore_from_checkpoint__(cls, pickled_checkpoint):
            checkpoint = pickle.loads(pickled_checkpoint)
            if hasattr(cls, "__ray_restore__"):
                actor_object = cls.__new__(cls)
                actor_object.__ray_restore__(checkpoint)
            else:
                # TODO(rkn): It's possible that this will cause problems. When
                # you unpickle the same object twice, the two objects will not
                # have the same class.
                actor_object = checkpoint
            return actor_object

        def __ray_checkpoint__(self, task_counter, previous_object_id):
            previous_object_id = previous_object_id[0]
            if previous_object_id is None:
                return False

            worker = ray.worker.global_worker
            plasma_id = plasma.ObjectID(previous_object_id.id())
            if previous_object_id in worker.actor_pinned_objects:
                print("Saving actor checkpoint. actor_counter = {}."
                      .format(task_counter))
                actor_key = b"Actor:" + worker.actor_id
                checkpoint = worker.actors[
                    worker.actor_id].__ray_save_checkpoint__()
                # Save the checkpoint in Redis. TODO(rkn): Checkpoints should
                # not be stored in Redis. Fix this.
                worker.redis_client.hset(
                    actor_key,
                    "checkpoint_{}".format(task_counter),
                    checkpoint)
                # Remove the previous checkpoints if there is one.
                checkpoint_indices = get_checkpoint_indices(worker,
                                                            worker.actor_id)
                for index in checkpoint_indices:
                    if index < task_counter:
                        worker.redis_client.hdel(
                            actor_key, "checkpoint_{}".format(index))
                return True
            else:
                checkpoint_index, checkpoint = get_actor_checkpoint(
                    worker, worker.actor_id)
                if checkpoint_index == task_counter:
                    actor = worker.actor_class.__ray_restore_from_checkpoint__(
                        checkpoint)
                    worker.actors[worker.actor_id] = actor
                    return True
                elif checkpoint_index > task_counter:
                    return False
                else:
                    worker.local_scheduler_client.reconstruct_object(
                        plasma_id.binary())
                    worker.local_scheduler_client.notify_unblocked()
                    return False

    Class.__module__ = cls.__module__
    Class.__name__ = cls.__name__

    class_id = random_actor_class_id()
    # The list exported will have length 0 if the class has not been exported
    # yet, and length one if it has. This is just implementing a bool, but we
    # don't use a bool because we need to modify it inside of the ActorHandle
    # constructor.
    exported = []

    # Create objects to wrap method invocations. This is done so that we can
    # invoke methods with actor.method.remote() instead of actor.method().
    class ActorMethod(object):
        def __init__(self, actor, method_name, method_signature):
            self.actor = actor
            self.method_name = method_name
            self.method_signature = method_signature

        def __call__(self, *args, **kwargs):
            raise Exception("Actor methods cannot be called directly. Instead "
                            "of running 'object.{}()', try "
                            "'object.{}.remote()'."
                            .format(self.method_name, self.method_name))

        def remote(self, *args, **kwargs):
            return self.actor._actor_method_call(self.method_name,
                                                 self.method_signature, *args,
                                                 **kwargs)

    class CheckpointMethod(ActorMethod):
        def remote(self):
            args = [self.actor._ray_actor_counter,
                    [self.actor._ray_actor_cursor]]
            return self.actor._actor_method_call(self.method_name,
                                                 self.method_signature, *args)

    class ActorHandle(object):
        def __init__(self, *args, **kwargs):
            raise Exception("Actor classes cannot be instantiated directly. "
                            "Instead of running '{}()', try '{}.remote()'."
                            .format(Class.__name__, Class.__name__))

        @classmethod
        def remote(cls, *args, **kwargs):
            actor_object = cls.__new__(cls)
            actor_object._manual_init(*args, **kwargs)
            return actor_object

        def _manual_init(self, *args, **kwargs):
            self._ray_actor_id = random_actor_id()
            # The number of actor method invocations that we've called so far.
            self._ray_actor_counter = 0
            # The actor cursor is a dummy object representing the most recent
            # actor method invocation. For each subsequent method invocation,
            # the current cursor should be added as a dependency, and then
            # updated to reflect the new invocation.
            self._ray_actor_cursor = None
            ray_actor_methods = inspect.getmembers(
                Class, predicate=(lambda x: (inspect.isfunction(x) or
                                             inspect.ismethod(x))))
            self._ray_actor_methods = {}
            for actor_method_name, actor_method in ray_actor_methods:
                self._ray_actor_methods[actor_method_name] = actor_method
            # Extract the signatures of each of the methods. This will be used
            # to catch some errors if the methods are called with inappropriate
            # arguments.
            self._ray_method_signatures = dict()
            for k, v in self._ray_actor_methods.items():
                # Print a warning message if the method signature is not
                # supported. We don't raise an exception because if the actor
                # inherits from a class that has a method whose signature we
                # don't support, we there may not be much the user can do about
                # it.
                signature.check_signature_supported(v, warn=True)
                self._ray_method_signatures[k] = signature.extract_signature(
                    v, ignore_first=True)

            # Do not export the actor class or the actor if run in PYTHON_MODE
            # Instead, instantiate the actor locally and add it to
            # global_worker's dictionary
            if ray.worker.global_worker.mode == ray.PYTHON_MODE:
                ray.worker.global_worker.actors[self._ray_actor_id] = (
                    Class.__new__(Class))
            else:
                # Export the actor class if it has not been exported yet.
                if len(exported) == 0:
                    export_actor_class(class_id, Class,
                                       self._ray_actor_methods.keys(),
                                       checkpoint_interval,
                                       ray.worker.global_worker)
                    exported.append(0)
                # Export the actor.
                export_actor(self._ray_actor_id, class_id,
                             self._ray_actor_methods.keys(), num_cpus,
                             num_gpus, ray.worker.global_worker)

            # Call __init__ as a remote function.
            if "__init__" in self._ray_actor_methods.keys():
                self._actor_method_call(
                    "__init__", self._ray_method_signatures["__init__"], *args,
                    **kwargs)
            else:
                print("WARNING: this object has no __init__ method.")

        # The function actor_method_call gets called if somebody tries to call
        # a method on their local actor stub object.
        def _actor_method_call(self, actor_method_name, function_signature,
                               *args, **kwargs):
            is_checkpoint = (actor_method_name == "__ray_checkpoint__")
            ray.worker.check_connected()
            ray.worker.check_main_thread()
            args = signature.extend_args(function_signature, args, kwargs)

            # Execute functions locally if Ray is run in PYTHON_MODE
            # Copy args to prevent the function from mutating them.
            if ray.worker.global_worker.mode == ray.PYTHON_MODE:
                return getattr(
                    ray.worker.global_worker.actors[self._ray_actor_id],
                    actor_method_name)(*copy.deepcopy(args))

            # Add the current actor cursor, a dummy object returned by the most
            # recent method invocation, as a dependency for the next method
            # invocation. Checkpoint methods receive the current actor cursor
            # as an implicit dependency (passed inside of a list).
            if not is_checkpoint and self._ray_actor_cursor is not None:
                args.append(self._ray_actor_cursor)

            if is_checkpoint:
                actor_counter = self._ray_actor_counter * -1
            else:
                actor_counter = self._ray_actor_counter

            function_id = get_actor_method_function_id(actor_method_name)
            object_ids = ray.worker.global_worker.submit_task(
                function_id, args, actor_id=self._ray_actor_id,
                actor_counter=actor_counter)
            # Update the actor counter and cursor to reflect the most recent
            # invocation.
            self._ray_actor_counter += 1
            self._ray_actor_cursor = object_ids.pop()

            if checkpoint_interval > 0:
                if (self._ray_actor_counter > 0 and self._ray_actor_counter %
                        checkpoint_interval == 0):
                    self.__ray_checkpoint__.remote()


            if len(object_ids) == 1:
                return object_ids[0]
            elif len(object_ids) > 1:
                return object_ids

        # Make tab completion work.
        def __dir__(self):
            return self._ray_actor_methods

        def __getattribute__(self, attr):
            # The following is needed so we can still access
            # self.actor_methods.
            if attr in ["_manual_init", "_ray_actor_id", "_ray_actor_counter",
                        "_ray_actor_cursor", "_ray_actor_methods",
                        "_actor_method_invokers", "_ray_method_signatures",
                        "_actor_method_call"]:
                return object.__getattribute__(self, attr)
            if attr in self._ray_actor_methods.keys():
                # We create the ActorMethod on the fly here so that the
                # ActorHandle doesn't need a reference to the ActorMethod. The
                # ActorMethod has a reference to the ActorHandle and this was
                # causing cyclic references which were prevent object
                # deallocation from behaving in a predictable manner.
                if attr == "__ray_checkpoint__":
                    actor_method_cls = CheckpointMethod
                else:
                    actor_method_cls = ActorMethod
                return actor_method_cls(self, attr,
                                        self._ray_method_signatures[attr])
            else:
                # There is no method with this name, so raise an exception.
                raise AttributeError("'{}' Actor object has no attribute '{}'"
                                     .format(Class, attr))

        def __repr__(self):
            return "Actor(" + self._ray_actor_id.hex() + ")"

        def __reduce__(self):
            raise Exception("Actor objects cannot be pickled.")

        def __del__(self):
            """Kill the worker that is running this actor."""
            if ray.worker.global_worker.connected:
                self._actor_method_call(
                    "__ray_terminate__",
                    self._ray_method_signatures["__ray_terminate__"],
                    self._ray_actor_id.id())

    return ActorHandle


ray.worker.global_worker.fetch_and_register_actor = fetch_and_register_actor
ray.worker.global_worker.make_actor = make_actor
