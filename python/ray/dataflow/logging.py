import logging
import sys
import threading
import time
import traceback

import redis

import ray.gcs_utils
import ray.local_scheduler
import ray.ray_constants as ray_constants
import ray.services as services

from ray.utils import _random_string

ERROR_KEY_PREFIX = b"Error:"
ERROR_KEYS = "ErrorKeys"
NIL_JOB_ID = ray_constants.ID_SIZE * b"\x00"


def format_error_message(exception_message, task_exception=False):
    """Improve the formatting of an exception thrown by a remote function.

    This method takes a traceback from an exception and makes it nicer by
    removing a few uninformative lines and adding some space to indent the
    remaining lines nicely.

    Args:
        exception_message (str): A message generated by traceback.format_exc().

    Returns:
        A string of the formatted exception message.
    """
    lines = exception_message.split("\n")
    if task_exception:
        # For errors that occur inside of tasks, remove lines 1 and 2 which are
        # always the same, they just contain information about the worker code.
        lines = lines[0:1] + lines[3:]
        pass
    return "\n".join(lines)


class LocalLogger(object):
    def __init__(self, worker):
        self.worker = worker
        self.logger = logging.getLogger("ray_worker:" +
                                        self.worker.worker_id.hex())

        self.log_stdout_file = None
        self.log_stderr_file = None

    def adjust_logger(self, level=logging.INFO, stream=None):
        self.logger.setLevel(level)
        if stream is None:
            stream = sys.stderr
        ch = logging.StreamHandler(stream)
        ch.setLevel(level)
        self.logger.addHandler(ch)

    @property
    def redirected(self):
        return (self.log_stdout_file is not None
                or self.log_stderr_file is not None)

    @property
    def use_raylet(self):
        return self.worker.use_raylet

    @property
    def redis_client(self):
        return self.worker.redis_client

    def redirect_logging_output(self):
        # TODO(suquark): Could we just redirect output of logger instead of
        # the standard I/O?

        # This key is set in services.py when Redis is started.
        redirect_worker_output_val = self.redis_client.get("RedirectOutput")
        if (redirect_worker_output_val is not None
                and int(redirect_worker_output_val) == 1):
            log_stdout_file, log_stderr_file = services.new_log_files(
                "worker", True)
            self.log_stdout_file = log_stdout_file
            self.log_stderr_file = log_stderr_file
            sys.stdout = log_stdout_file
            sys.stderr = log_stderr_file
            services.record_log_files_in_redis(
                self.worker.redis_address, self.worker.node_ip_address,
                [log_stdout_file, log_stderr_file])


class WorkerLogger(LocalLogger):
    def __init__(self, worker, global_state):
        """Initialize the worker logger.

        Args:
            worker: The worker to use.
        """

        super(WorkerLogger, self).__init__(worker)
        self.global_state = global_state
        self.error_message_pubsub_client = None

    @property
    def lock(self):
        # TODO: Could we use a separate lock to improve efficiency?
        return self.worker.lock

    @property
    def task_driver_id(self):
        return self.worker.task_driver_id

    def push_error_to_driver(self,
                             error_type,
                             message,
                             driver_id=None,
                             data=None,
                             force_redis=False):
        """Push an error message to the driver to be printed in the background.

        Args:
            error_type (str): The type of the error.
            message (str): The message that will be printed in the background
                on the driver.
            driver_id: The ID of the driver to push the error message to.
                If this is None,
                then the message will be pushed to all drivers.
            data: This should be a dictionary mapping strings to strings. It
                will be serialized with json and stored in Redis.
            force_redis: Normally the push_error_to_driver function should be used.
                However, in some instances,
                the local scheduler client is not available, e.g.,
                because the error happens in Python before the driver or worker has
                connected to the backend processes.
        """

        if driver_id is None:
            driver_id = ray_constants.NIL_JOB_ID.id()
        error_key = ERROR_KEY_PREFIX + driver_id + b":" + _random_string()
        data = {} if data is None else data
        if not self.use_raylet:
            self.redis_client.hmset(error_key, {
                "type": error_type,
                "message": message,
                "data": data
            })
            self.redis_client.rpush("ErrorKeys", error_key)
        else:
            if force_redis:
                # Do everything in Python and through the Python Redis client
                # instead of through the raylet.
                error_data = ray.gcs_utils.construct_error_message(
                    error_type, message, time.time())
                self.redis_client.execute_command(
                    "RAY.TABLE_APPEND", ray.gcs_utils.TablePrefix.ERROR_INFO,
                    ray.gcs_utils.TablePubsub.ERROR_INFO, driver_id,
                    error_data)
            else:
                self.local_scheduler_client.push_error(
                    ray.ObjectID(driver_id), error_type, message, time.time())

    def push_exception_to_driver(self,
                                 error_type,
                                 driver_id=None,
                                 data=None,
                                 force_redis=False,
                                 format_exc=False):

        traceback_str = traceback.format_exc()
        if format_exc:
            traceback_str = format_error_message(traceback_str)
        self.push_error_to_driver(
            error_type,
            traceback_str,
            driver_id=driver_id,
            data=data,
            force_redis=force_redis,
        )

    def error_applies_to_driver(self, error_key):
        """Return True if the error is for this driver and false otherwise."""
        # TODO(rkn): Should probably check that this is only called on a driver.
        # Check that the error key is formatted as in push_error_to_driver.
        assert len(error_key) == (len(ERROR_KEY_PREFIX) + ray_constants.ID_SIZE
                                  + 1 + ray_constants.ID_SIZE), error_key

        driver_id = error_key[len(ERROR_KEY_PREFIX):(
            len(ERROR_KEY_PREFIX) + ray_constants.ID_SIZE)]
        # If the driver ID in the error message is a sequence of all zeros, then
        # the message is intended for all drivers.
        return driver_id in (self.task_driver_id.id(), NIL_JOB_ID)

    def print_error_messages_raylet(self):
        """Print error messages in the background on the driver.

        This runs in a separate thread on the driver and prints error messages in
        the background.
        """
        if not self.use_raylet:
            raise Exception(
                "This function is specific to the raylet code path.")

        self.error_message_pubsub_client = self.redis_client.pubsub(
            ignore_subscribe_messages=True)
        # Exports that are published after the call to
        # error_message_pubsub_client.subscribe and before the call to
        # error_message_pubsub_client.listen will still be processed in the loop.

        # Really we should just subscribe to the errors for this specific job.
        # However, currently all errors seem to be published on the same channel.
        error_pubsub_channel = str(
            ray.gcs_utils.TablePubsub.ERROR_INFO).encode("ascii")
        self.error_message_pubsub_client.subscribe(error_pubsub_channel)
        # worker.error_message_pubsub_client.psubscribe("*")

        # Keep a set of all the error messages that we've seen so far in order to
        # avoid printing the same error message repeatedly. This is especially
        # important when running a script inside of a tool like screen where
        # scrolling is difficult.
        old_error_messages = set()

        # Get the exports that occurred before the call to subscribe.
        with self.lock:
            error_messages = self.global_state.error_messages(
                self.task_driver_id)
            for error_message in error_messages:
                if error_message not in old_error_messages:
                    self.logger.error(error_message)
                    old_error_messages.add(error_message)
                else:
                    self.logger.error("Suppressing duplicate error message.")

        try:
            for msg in self.error_message_pubsub_client.listen():

                gcs_entry = ray.gcs_utils.GcsTableEntry.GetRootAsGcsTableEntry(
                    msg["data"], 0)
                assert gcs_entry.EntriesLength() == 1
                error_data = ray.gcs_utils.ErrorTableData.GetRootAsErrorTableData(
                    gcs_entry.Entries(0), 0)

                job_id = error_data.JobId()
                if job_id not in [self.task_driver_id.id(), NIL_JOB_ID]:
                    continue

                error_message = ray.utils.decode(error_data.ErrorMessage())

                if error_message not in old_error_messages:
                    self.logger.error(error_message)
                    old_error_messages.add(error_message)
                else:
                    self.logger.error("Suppressing duplicate error message.")

        except redis.ConnectionError:
            # When Redis terminates the listen call will throw a ConnectionError,
            # which we catch here.
            pass

    def print_error_messages(self):
        """Print error messages in the background on the driver.

        This runs in a separate thread on the driver and prints error messages in
        the background.
        """
        # TODO(rkn): All error messages should have a "component" field indicating
        # which process the error came from (e.g., a worker or a plasma store).
        # Currently all error messages come from workers.

        self.error_message_pubsub_client = self.redis_client.pubsub()
        # Exports that are published after the call to
        # error_message_pubsub_client.subscribe and before the call to
        # error_message_pubsub_client.listen will still be processed in the loop.
        self.error_message_pubsub_client.subscribe("__keyspace@0__:ErrorKeys")
        num_errors_received = 0

        # Keep a set of all the error messages that we've seen so far in order to
        # avoid printing the same error message repeatedly. This is especially
        # important when running a script inside of a tool like screen where
        # scrolling is difficult.
        old_error_messages = set()

        # Get the exports that occurred before the call to subscribe.
        with self.lock:
            error_keys = self.redis_client.lrange(ERROR_KEYS, 0, -1)
            for error_key in error_keys:
                if self.error_applies_to_driver(error_key):
                    error_message = ray.utils.decode(
                        self.redis_client.hget(error_key, "message"))
                    if error_message not in old_error_messages:
                        self.logger.error(error_message)
                        old_error_messages.add(error_message)
                    else:
                        self.logger.error(
                            "Suppressing duplicate error message.")
                num_errors_received += 1

        try:
            for msg in self.error_message_pubsub_client.listen():
                with self.lock:
                    for error_key in self.redis_client.lrange(
                            ERROR_KEYS, num_errors_received, -1):
                        if self.error_applies_to_driver(error_key):
                            error_message = ray.utils.decode(
                                self.redis_client.hget(error_key, "message"))
                            if error_message not in old_error_messages:
                                self.logger.error(error_message)
                                old_error_messages.add(error_message)
                            else:
                                self.logger.error(
                                    "Suppressing duplicate error message.")
                        num_errors_received += 1
        except redis.ConnectionError:
            # When Redis terminates the listen call will throw a ConnectionError,
            # which we catch here.
            pass

    def error_info(self):
        """Return information about failed tasks."""
        if self.use_raylet:
            return (
                self.global_state.error_messages(job_id=self.task_driver_id) +
                self.global_state.error_messages(
                    job_id=ray_constants.NIL_JOB_ID))
        error_keys = self.redis_client.lrange(ERROR_KEYS, 0, -1)
        errors = []
        for error_key in error_keys:
            if self.error_applies_to_driver(error_key):
                error_contents = self.redis_client.hgetall(error_key)
                error_contents = {
                    "type": ray.utils.decode(error_contents[b"type"]),
                    "message": ray.utils.decode(error_contents[b"message"]),
                    "data": ray.utils.decode(error_contents[b"data"])
                }
                errors.append(error_contents)

        return errors

    def start_logging_thread(self):
        if not self.use_raylet:
            t = threading.Thread(target=self.print_error_messages)
        else:
            t = threading.Thread(target=self.print_error_messages_raylet)
        # Making the thread a daemon causes it to exit when the main thread
        # exits.
        t.daemon = True
        t.start()
