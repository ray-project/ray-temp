# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import funcsigs
import numpy

from libc.stdint cimport int32_t, int64_t
from libcpp cimport bool as c_bool
from libcpp.memory cimport unique_ptr
from libcpp.string cimport string as c_string
from libcpp.utility cimport pair
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector as c_vector

from cython.operator import dereference, postincrement

from ray.includes.common cimport (
    CLanguage,
    CRayStatus,
    LANGUAGE_CPP,
    LANGUAGE_JAVA,
    LANGUAGE_PYTHON,
)
from ray.includes.libraylet cimport (
    CRayletClient,
    GCSProfileEventT,
    GCSProfileTableDataT,
    ResourceMappingType,
    WaitResultPair,
)
from ray.includes.unique_ids cimport (
    CActorCheckpointID,
    CObjectID,
)
from ray.includes.task cimport CTaskSpecification
from ray.includes.ray_config cimport RayConfig
from ray.utils import decode

cimport cpython

include "includes/profiling.pxi"
include "includes/unique_ids.pxi"
include "includes/ray_config.pxi"
include "includes/task.pxi"


if cpython.PY_MAJOR_VERSION >= 3:
    import pickle
else:
    import cPickle as pickle


cdef int check_status(const CRayStatus& status) nogil except -1:
    if status.ok():
        return 0

    with gil:
        message = status.message().decode()
        raise Exception(message)


cdef c_vector[CObjectID] ObjectIDsToVector(object_ids):
    """A helper function that converts a Python list of object IDs to a vector.

    Args:
        object_ids (list): The Python list of object IDs.

    Returns:
        The output vector.
    """
    cdef:
        ObjectID object_id
        c_vector[CObjectID] result
    for object_id in object_ids:
        result.push_back(object_id.native())
    return result


cdef VectorToObjectIDs(c_vector[CObjectID] object_ids):
    result = []
    for i in range(object_ids.size()):
        result.append(ObjectID(object_ids[i].binary()))
    return result


def compute_put_id(TaskID task_id, int64_t put_index):
    if put_index < 1 or put_index > kMaxTaskPuts:
        raise ValueError("The range of 'put_index' should be [1, %d]"
                         % kMaxTaskPuts)
    return ObjectID(ComputePutId(task_id.native(), put_index).binary())


def compute_task_id(ObjectID object_id):
    return TaskID(ComputeTaskId(object_id.native()).binary())


cdef c_bool is_simple_value(value, int *num_elements_contained):
    num_elements_contained[0] += 1

    if num_elements_contained[0] >= RayConfig.instance().num_elements_limit():
        return False

    if (cpython.PyInt_Check(value) or cpython.PyLong_Check(value) or
            value is False or value is True or cpython.PyFloat_Check(value) or
            value is None):
        return True

    if cpython.PyBytes_CheckExact(value):
        num_elements_contained[0] += cpython.PyBytes_Size(value)
        return (num_elements_contained[0] <
                RayConfig.instance().num_elements_limit())

    if cpython.PyUnicode_CheckExact(value):
        num_elements_contained[0] += cpython.PyUnicode_GET_SIZE(value)
        return (num_elements_contained[0] <
                RayConfig.instance().num_elements_limit())

    if (cpython.PyList_CheckExact(value) and
            cpython.PyList_Size(value) < RayConfig.instance().size_limit()):
        for item in value:
            if not is_simple_value(item, num_elements_contained):
                return False
        return (num_elements_contained[0] <
                RayConfig.instance().num_elements_limit())

    if (cpython.PyDict_CheckExact(value) and
            cpython.PyDict_Size(value) < RayConfig.instance().size_limit()):
        # TODO(suquark): Using "items" in Python2 is not very efficient.
        for k, v in value.items():
            if not (is_simple_value(k, num_elements_contained) and
                    is_simple_value(v, num_elements_contained)):
                return False
        return (num_elements_contained[0] <
                RayConfig.instance().num_elements_limit())

    if (cpython.PyTuple_CheckExact(value) and
            cpython.PyTuple_Size(value) < RayConfig.instance().size_limit()):
        for item in value:
            if not is_simple_value(item, num_elements_contained):
                return False
        return (num_elements_contained[0] <
                RayConfig.instance().num_elements_limit())

    if isinstance(value, numpy.ndarray):
        if value.dtype == "O":
            return False
        num_elements_contained[0] += value.nbytes
        return (num_elements_contained[0] <
                RayConfig.instance().num_elements_limit())

    return False


def check_simple_value(value):
    """Check if value is simple enough to be send by value.

    This method checks if a Python object is sufficiently simple that it can
    be serialized and passed by value as an argument to a task (without being
    put in the object store). The details of which objects are sufficiently
    simple are defined by this method and are not particularly important.
    But for performance reasons, it is better to place "small" objects in
    the task itself and "large" objects in the object store.

    Args:
        value: Python object that should be checked.

    Returns:
        True if the value should be send by value, False otherwise.
    """

    cdef int num_elements_contained = 0
    return is_simple_value(value, &num_elements_contained)


cdef class Language:
    cdef CLanguage lang

    def __cinit__(self, int32_t lang):
        self.lang = <CLanguage>lang

    @staticmethod
    cdef from_native(const CLanguage& lang):
        return Language(<int32_t>lang)

    def __eq__(self, other):
        return (isinstance(other, Language) and
                (<int32_t>self.lang) == (<int32_t>other.lang))

    def __repr__(self):
        if <int32_t>self.lang == <int32_t>LANGUAGE_PYTHON:
            return "PYTHON"
        elif <int32_t>self.lang == <int32_t>LANGUAGE_CPP:
            return "CPP"
        elif <int32_t>self.lang == <int32_t>LANGUAGE_JAVA:
            return "JAVA"
        else:
            raise Exception("Unexpected error")


# Programming language enum values.
cdef Language LANG_PYTHON = Language.from_native(LANGUAGE_PYTHON)
cdef Language LANG_CPP = Language.from_native(LANGUAGE_CPP)
cdef Language LANG_JAVA = Language.from_native(LANGUAGE_JAVA)


cdef unordered_map[c_string, double] resource_map_from_dict(resource_map):
    cdef:
        unordered_map[c_string, double] out
        c_string resource_name
    if not isinstance(resource_map, dict):
        raise TypeError("resource_map must be a dictionary")
    for key, value in resource_map.items():
        out[key.encode("ascii")] = float(value)
    return out


cdef extend_args(function_signature, args, kwargs):
    """Extend the arguments that were passed into a function.

    This extends the arguments that were passed into a function with the
    default arguments provided in the function definition.

    Args:
        function_signature: The function signature of the function being
            called.
        args: The non-keyword arguments passed into the function.
        kwargs: The keyword arguments passed into the function.

    Returns:
        An extended list of arguments to pass into the function.

    Raises:
        Exception: An exception may be raised if the function cannot be called
            with these arguments.
    """
    arg_names = function_signature.arg_names
    arg_defaults = function_signature.arg_defaults
    arg_is_positionals = function_signature.arg_is_positionals
    function_name = function_signature.function_name

    args = list(args)

    for keyword_name in kwargs:
        if keyword_name not in function_signature.keyword_names:
            raise Exception("The name '{}' is not a valid keyword argument "
                            "for the function '{}'.".format(
                                keyword_name, function_name))

    # Fill in the remaining arguments.
    for i in range(min(len(args), len(arg_names))):
        skipped_name = arg_names[i]
        if skipped_name in kwargs:
            raise Exception("Positional and keyword value provided for the "
                            "argument '{}' for the function '{}'".format(
                                keyword_name, function_name))

    for i in range(len(args), len(arg_names)):
        keyword_name = arg_names[i]
        default_value = arg_defaults[i]
        is_positional = arg_is_positionals[i]
        if keyword_name in kwargs:
            args.append(kwargs[keyword_name])
        else:
            if default_value != funcsigs._empty:
                args.append(default_value)
            else:
                # This means that there is a missing argument. Unless this is
                # the last argument and it is a *args argument in which case it
                # can be omitted.
                if not is_positional:
                    raise Exception("No value was provided for the argument "
                                    "'{}' for the function '{}'.".format(
                                        keyword_name, function_name))

    no_positionals = len(arg_is_positionals) == 0 or not arg_is_positionals[-1]
    too_many_arguments = len(args) > len(arg_names) and no_positionals
    if too_many_arguments:
        raise Exception("Too many arguments were passed to the function '{}'"
                        .format(function_name))
    return args


cdef class RayletClient:
    cdef unique_ptr[CRayletClient] client

    def __cinit__(self, raylet_socket,
                  ClientID client_id,
                  c_bool is_worker,
                  DriverID driver_id):
        # We know that we are using Python, so just skip the language
        # parameter.
        # TODO(suquark): Should we allow unicode chars in "raylet_socket"?
        self.client.reset(new CRayletClient(
            raylet_socket.encode("ascii"), client_id.native(), is_worker,
            driver_id.native(), LANGUAGE_PYTHON))

    def disconnect(self):
        check_status(self.client.get().Disconnect())

    def submit_task(self,
                    worker,
                    function_descriptor_list,
                    function_signature,
                    args,
                    kwargs,
                    TaskID current_task_id,
                    ActorID actor_id=ActorID.nil(),
                    ActorHandleID actor_handle_id=ActorHandleID.nil(),
                    int actor_counter=0,
                    ActorID actor_creation_id=ActorID.nil(),
                    ObjectID actor_creation_dummy_object_id=ObjectID.nil(),
                    int max_actor_reconstructions=0,
                    execution_dependencies=None,
                    new_actor_handles=None,
                    num_return_vals=None,
                    resources=None,
                    placement_resources=None,
                    DriverID driver_id=DriverID.nil()):
        """Submit a remote task to the scheduler.

        Tell the scheduler to schedule the execution of the function with
        function_descriptor with arguments args. Retrieve object IDs for the
        outputs of the function from the scheduler and immediately return them.

        Args:
            worker: The worker that submitted this task.
            function_descriptor_list: The function descriptor list to execute.
            function_signature: The signature of the submitted function.
            args: The arguments to pass into the function. Arguments can
                be object IDs or they can be values. If they are values, they
                must be serializable objects.
            kwargs: The keyword arguments to pass into the function.
            current_task_id: The ID of the parent task.
            actor_id: The ID of the actor that this task is for.
            actor_counter: The counter of the actor task.
            actor_creation_id: The ID of the actor to create, if this is an
                actor creation task.
            actor_creation_dummy_object_id: If this task is an actor method,
                then this argument is the dummy object ID associated with the
                actor creation task for the corresponding actor.
            max_actor_reconstructions: If this number of 0 or negative,
                the actor won't be reconstructed on failure.
            execution_dependencies: The execution dependencies for this task.
            new_actor_handles: If this is an actor task, then this will be
                populated with all of the new actor handles that were forked
                from this handle since the last task on this handle was
                submitted.
            num_return_vals: The number of return values this function should
                have.
            resources: The resource requirements for this task.
            placement_resources: The resources required for placing the task.
                If this is not provided or if it is an empty dictionary, then
                the placement resources will be equal to resources.
            driver_id: The ID of the relevant driver. This is almost always the
                driver ID of the driver that is currently running. However, in
                the exceptional case that an actor task is being dispatched to
                an actor created by a different driver, this should be the
                driver ID of the driver that created the actor.

        Returns:
            The return object IDs for this task.
        """

        cdef Task task
        with profile("submit_task"):
            worker.task_context.task_index += 1
            if driver_id.is_nil():
                driver_id = worker.task_driver_id
            kwargs = {} if kwargs is None else kwargs
            args = [] if args is None else args

            args = extend_args(function_signature, args, kwargs)

            args_for_raylet = []
            for arg in args:
                if isinstance(arg, ObjectID):
                    args_for_raylet.append(arg)
                elif check_simple_value(arg):
                    args_for_raylet.append(arg)
                else:
                    args_for_raylet.append(ray.worker.put(arg))

            if execution_dependencies is None:
                execution_dependencies = []

            if new_actor_handles is None:
                new_actor_handles = []

            if resources is None:
                raise ValueError("The resources dictionary is required.")
            for value in resources.values():
                assert (isinstance(value, int) or isinstance(value, float))
                if value < 0:
                    raise ValueError(
                        "Resource quantities must be nonnegative.")
                if (value >= 1 and isinstance(value, float)
                        and not value.is_integer()):
                    raise ValueError(
                        "Resource quantities must all be whole numbers.")

            # Remove any resources with zero quantity requirements
            resources = {
                resource_label: resource_quantity
                for resource_label, resource_quantity in resources.items()
                if resource_quantity > 0
            }

            if placement_resources is None:
                placement_resources = {}

            task = Task(
                driver_id,
                function_descriptor_list,
                args_for_raylet,
                num_return_vals,
                current_task_id,
                worker.task_context.task_index,
                actor_creation_id,
                actor_creation_dummy_object_id,
                max_actor_reconstructions,
                actor_id,
                actor_handle_id,
                actor_counter,
                new_actor_handles,
                execution_dependencies,
                resources,
                placement_resources)

            check_status(self.client.get().SubmitTask(
                task.execution_dependencies.get()[0],
                task.task_spec.get()[0]))

            object_ids = task.returns()
            if len(object_ids) == 1:
                return object_ids[0]
            elif len(object_ids) > 1:
                return object_ids

    def get_task(self):
        cdef:
            unique_ptr[CTaskSpecification] task_spec

        with nogil:
            check_status(self.client.get().GetTask(&task_spec))
        return Task.make(task_spec)

    def task_done(self):
        check_status(self.client.get().TaskDone())

    def fetch_or_reconstruct(self, object_ids,
                             c_bool fetch_only,
                             TaskID current_task_id=TaskID.nil()):
        cdef c_vector[CObjectID] fetch_ids = ObjectIDsToVector(object_ids)
        check_status(self.client.get().FetchOrReconstruct(
            fetch_ids, fetch_only, current_task_id.native()))

    def notify_unblocked(self, TaskID current_task_id):
        check_status(self.client.get().NotifyUnblocked(current_task_id.native()))

    def wait(self, object_ids, int num_returns, int64_t timeout_milliseconds,
             c_bool wait_local, TaskID current_task_id):
        cdef:
            WaitResultPair result
            c_vector[CObjectID] wait_ids
            CTaskID c_task_id = current_task_id.native()
        wait_ids = ObjectIDsToVector(object_ids)
        with nogil:
            check_status(self.client.get().Wait(wait_ids, num_returns,
                                                timeout_milliseconds,
                                                wait_local,
                                                c_task_id, &result))
        return (VectorToObjectIDs(result.first),
                VectorToObjectIDs(result.second))

    def resource_ids(self):
        cdef:
            ResourceMappingType resource_mapping = (
                self.client.get().GetResourceIDs())
            unordered_map[
                c_string, c_vector[pair[int64_t, double]]
            ].iterator iterator = resource_mapping.begin()
            c_vector[pair[int64_t, double]] c_value
        resources_dict = {}
        while iterator != resource_mapping.end():
            key = decode(dereference(iterator).first)
            c_value = dereference(iterator).second
            ids_and_fractions = []
            for i in range(c_value.size()):
                ids_and_fractions.append(
                    (c_value[i].first, c_value[i].second))
            resources_dict[key] = ids_and_fractions
            postincrement(iterator)
        return resources_dict

    def push_error(self, DriverID driver_id, error_type, error_message,
                   double timestamp):
        check_status(self.client.get().PushError(driver_id.native(),
                                                 error_type.encode("ascii"),
                                                 error_message.encode("ascii"),
                                                 timestamp))

    def push_profile_events(self, component_type, UniqueID component_id,
                            node_ip_address, profile_data):
        cdef:
            GCSProfileTableDataT profile_info
            GCSProfileEventT *profile_event
            c_string event_type

        if len(profile_data) == 0:
            return  # Short circuit if there are no profile events.

        profile_info.component_type = component_type.encode("ascii")
        profile_info.component_id = component_id.binary()
        profile_info.node_ip_address = node_ip_address.encode("ascii")

        for py_profile_event in profile_data:
            profile_event = new GCSProfileEventT()
            if not isinstance(py_profile_event, dict):
                raise TypeError(
                    "Incorrect type for a profile event. Expected dict "
                    "instead of '%s'" % str(type(py_profile_event)))
            # TODO(rkn): If the dictionary is formatted incorrectly, that
            # could lead to errors. E.g., if any of the strings are empty,
            # that will cause segfaults in the node manager.
            for key_string, event_data in py_profile_event.items():
                if key_string == "event_type":
                    profile_event.event_type = event_data.encode("ascii")
                    if profile_event.event_type.length() == 0:
                        raise ValueError(
                            "'event_type' should not be a null string.")
                elif key_string == "start_time":
                    profile_event.start_time = float(event_data)
                elif key_string == "end_time":
                    profile_event.end_time = float(event_data)
                elif key_string == "extra_data":
                    profile_event.extra_data = event_data.encode("ascii")
                    if profile_event.extra_data.length() == 0:
                        raise ValueError(
                            "'extra_data' should not be a null string.")
                else:
                    raise ValueError(
                        "Unknown profile event key '%s'" % key_string)
            # Note that profile_info.profile_events is a vector of unique
            # pointers, so profile_event will be deallocated when profile_info
            # goes out of scope. "emplace_back" of vector has not been
            # supported by Cython
            profile_info.profile_events.push_back(
                unique_ptr[GCSProfileEventT](profile_event))

        check_status(self.client.get().PushProfileEvents(profile_info))

    def free_objects(self, object_ids, c_bool local_only):
        cdef c_vector[CObjectID] free_ids = ObjectIDsToVector(object_ids)
        check_status(self.client.get().FreeObjects(free_ids, local_only))

    def prepare_actor_checkpoint(self, ActorID actor_id):
        cdef CActorCheckpointID checkpoint_id
        cdef CActorID c_actor_id = actor_id.native()
        # PrepareActorCheckpoint will wait for raylet's reply, release
        # the GIL so other Python threads can run.
        with nogil:
            check_status(self.client.get().PrepareActorCheckpoint(
                c_actor_id, checkpoint_id))
        return ActorCheckpointID(checkpoint_id.binary())

    def notify_actor_resumed_from_checkpoint(self, ActorID actor_id,
                                             ActorCheckpointID checkpoint_id):
        check_status(self.client.get().NotifyActorResumedFromCheckpoint(
            actor_id.native(), checkpoint_id.native()))

    @property
    def language(self):
        return Language.from_native(self.client.get().GetLanguage())

    @property
    def client_id(self):
        return ClientID(self.client.get().GetClientID().binary())

    @property
    def driver_id(self):
        return DriverID(self.client.get().GetDriverID().binary())

    @property
    def is_worker(self):
        return self.client.get().IsWorker()
