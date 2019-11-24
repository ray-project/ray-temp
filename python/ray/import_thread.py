from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import defaultdict
import threading
import traceback

import redis

import ray
from ray import ray_constants
from ray import cloudpickle as pickle
from ray import profiling
from ray import utils

import logging

logger = logging.getLogger(__name__)


class ImportThread(object):
    """A thread used to import exports from the driver or other workers.

    Note: The driver also has an import thread, which is used only to import
    custom class definitions from calls to _register_custom_serializer that
    happen under the hood on workers.

    Attributes:
        worker: the worker object in this process.
        mode: worker mode
        redis_client: the redis client used to query exports.
        threads_stopped (threading.Event): A threading event used to signal to
            the thread that it should exit.
        imported_function_bytecodes: This is a dicitonary mapping strings
            containing the bytecode of the remote functions that have been
            exported to the number of times each remote function has been
            imported. this is used to provide good error messages when the same
            function is exported many many times.
    """

    def __init__(self, worker, mode, threads_stopped):
        self.worker = worker
        self.mode = mode
        self.redis_client = worker.redis_client
        self.threads_stopped = threads_stopped
        # TODO(rkn): Should we do the same thing as imported_function_bytecodes
        # for actors as well?
        self.imported_function_bytecodes = defaultdict(int)

    def start(self):
        """Start the import thread."""
        self.t = threading.Thread(target=self._run, name="ray_import_thread")
        # Making the thread a daemon causes it to exit
        # when the main thread exits.
        self.t.daemon = True
        self.t.start()

    def join_import_thread(self):
        """Wait for the thread to exit."""
        self.t.join()

    def _run(self):
        import_pubsub_client = self.redis_client.pubsub()
        # Exports that are published after the call to
        # import_pubsub_client.subscribe and before the call to
        # import_pubsub_client.listen will still be processed in the loop.
        import_pubsub_client.subscribe("__keyspace@0__:Exports")
        # Keep track of the number of imports that we've imported.
        num_imported = 0

        try:
            # Get the exports that occurred before the call to subscribe.
            export_keys = self.redis_client.lrange("Exports", 0, -1)
            for key in export_keys:
                num_imported += 1
                self._process_key(key)

            while True:
                # Exit if we received a signal that we should stop.
                if self.threads_stopped.is_set():
                    return

                msg = import_pubsub_client.get_message()
                if msg is None:
                    self.threads_stopped.wait(timeout=0.01)
                    continue

                if msg["type"] == "subscribe":
                    continue
                assert msg["data"] == b"rpush"
                num_imports = self.redis_client.llen("Exports")
                assert num_imports >= num_imported
                for i in range(num_imported, num_imports):
                    num_imported += 1
                    key = self.redis_client.lindex("Exports", i)
                    self._process_key(key)
        except (OSError, redis.exceptions.ConnectionError) as e:
            logger.error("ImportThread: {}".format(e))
        finally:
            # Close the pubsub client to avoid leaking file descriptors.
            import_pubsub_client.close()

    def _process_key(self, key):
        """Process the given export key from redis."""
        # Handle the driver case first.
        if self.mode != ray.WORKER_MODE:
            if key.startswith(b"FunctionsToRun"):
                with profiling.profile("fetch_and_run_function"):
                    self.fetch_and_execute_function_to_run(key)

            # If the same remote function definition appears to be exported
            # many times, then print a warning. We only issue this warning from
            # the driver so that it is only triggered once instead of many
            # times.
            elif key.startswith(b"RemoteFunction"):
                function_bytecode, function_name = self.redis_client.hmget(
                    key, ["function_bytecode", "name"])
                function_identifier = (function_name, function_bytecode)
                self.imported_function_bytecodes[function_identifier] += 1
                if (self.imported_function_bytecodes[function_identifier] ==
                        ray_constants.DUPLICATE_REMOTE_FUNCTION_THRESHOLD):
                    logger.warning(
                        "The remote function '%s' has been exported %s "
                        "times. While this may not be an issue, this may "
                        "indicate that the same remote function is being "
                        "defined repeatedly from within many tasks and "
                        "exported to all of the workers. This can be a "
                        "performance issue and can be resolved by defining "
                        "the remote function on the driver instead. See "
                        "https://github.com/ray-project/ray/issues/6240 for "
                        "more discussion.", ray.utils.decode(function_name),
                        ray_constants.DUPLICATE_REMOTE_FUNCTION_THRESHOLD)

            # Return because FunctionsToRun are the only things that
            # the driver should import.
            return

        if key.startswith(b"RemoteFunction"):
            with profiling.profile("register_remote_function"):
                (self.worker.function_actor_manager.
                 fetch_and_register_remote_function(key))
        elif key.startswith(b"FunctionsToRun"):
            with profiling.profile("fetch_and_run_function"):
                self.fetch_and_execute_function_to_run(key)
        elif key.startswith(b"ActorClass"):
            # Keep track of the fact that this actor class has been
            # exported so that we know it is safe to turn this worker
            # into an actor of that class.
            self.worker.function_actor_manager.imported_actor_classes.add(key)
        # TODO(rkn): We may need to bring back the case of
        # fetching actor classes here.
        else:
            raise Exception("This code should be unreachable.")

    def fetch_and_execute_function_to_run(self, key):
        """Run on arbitrary function on the worker."""
        (job_id, serialized_function,
         run_on_other_drivers) = self.redis_client.hmget(
             key, ["job_id", "function", "run_on_other_drivers"])

        if (utils.decode(run_on_other_drivers) == "False"
                and self.worker.mode == ray.SCRIPT_MODE
                and job_id != self.worker.current_job_id.binary()):
            return

        try:
            # FunctionActorManager may call pickle.loads at the same time.
            # Importing the same module in different threads causes deadlock.
            with self.worker.function_actor_manager.lock:
                # Deserialize the function.
                function = pickle.loads(serialized_function)
            # Run the function.
            function({"worker": self.worker})
        except Exception:
            # If an exception was thrown when the function was run, we record
            # the traceback and notify the scheduler of the failure.
            traceback_str = traceback.format_exc()
            # Log the error message.
            utils.push_error_to_driver(
                self.worker,
                ray_constants.FUNCTION_TO_RUN_PUSH_ERROR,
                traceback_str,
                job_id=ray.JobID(job_id))
