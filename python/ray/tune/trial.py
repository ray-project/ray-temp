from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray.cloudpickle as cloudpickle
import copy
from datetime import datetime
import logging
import uuid
import time
import tempfile
import os
from numbers import Number
from ray.tune import TuneError
from ray.tune.checkpoint_manager import Checkpoint, CheckpointManager
from ray.tune.logger import pretty_print, UnifiedLogger
from ray.tune.syncer import get_syncer
from ray.tune.util import flatten_dict
# NOTE(rkn): We import ray.tune.registry here instead of importing the names we
# need because there are cyclic imports that may cause specific names to not
# have been defined yet. See https://github.com/ray-project/ray/issues/1716.
from ray.tune.registry import get_trainable_cls, validate_trainable
from ray.tune.result import DEFAULT_RESULTS_DIR, DONE, TRAINING_ITERATION
from ray.utils import binary_to_hex, hex_to_binary
from ray.tune.resources import Resources, json_to_resources, resources_to_json

DEBUG_PRINT_INTERVAL = 5
MAX_LEN_IDENTIFIER = int(os.environ.get("MAX_LEN_IDENTIFIER", 130))
logger = logging.getLogger(__name__)


def date_str():
    return datetime.today().strftime("%Y-%m-%d_%H-%M-%S")


class Location(object):
    """Describes the location at which Trial is placed to run."""

    def __init__(self, hostname=None, pid=None):
        self.hostname = hostname
        self.pid = pid

    def __str__(self):
        if not self.pid:
            return ""
        elif self.hostname == os.uname()[1]:
            return "pid={}".format(self.pid)
        else:
            return "{}:{}".format(self.hostname, self.pid)


class TrialDirSchema(object):
    """Describes the Trial's directory structure.

    {trial_dir_name}/
      driver.log.1
      driver.log.2
      remote_logs/
      checkpoints/
    """
    REMOTE_LOGDIR = "remote_logs"
    CHECKPOINT_DIR = "checkpoints"

    def __init__(self, trial_name, local_dir):
        local_dir = os.path.expanduser(local_dir)
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        self.root_dir = tempfile.mkdtemp(
            prefix="{}_{}".format(trial_name[:MAX_LEN_IDENTIFIER], date_str()),
            dir=local_dir)
        self.makedirs()

    def makedirs(self):
        for path in (self.root_dir, self.logdir, self.remote_logdir,
                     self.checkpoint_dir):
            if not os.path.exists(path):
                os.makedirs(path)

    @property
    def root(self):
        """Root directory."""
        return self.root_dir

    @property
    def logdir(self):
        """Directory containing logs that originated on the driver.

        For backwards compatibility, this is set to the root.
        """
        return self.root_dir

    @property
    def remote_logdir(self):
        """Directory containing logs that originated on the remote worker."""
        return os.path.join(self.root_dir, TrialDirSchema.REMOTE_LOGDIR)

    @property
    def checkpoint_dir(self):
        """Directory containing checkpoints taken on the remote worker."""
        return os.path.join(self.root_dir, TrialDirSchema.CHECKPOINT_DIR)


class ExportFormat(object):
    """Describes the format to export the trial Trainable.

    This may correspond to different file formats based on the
    Trainable implementation.
    """
    CHECKPOINT = "checkpoint"
    MODEL = "model"

    @staticmethod
    def validate(export_formats):
        """Validates export_formats.

        Raises:
            ValueError if the format is unknown.
        """
        for i in range(len(export_formats)):
            export_formats[i] = export_formats[i].strip().lower()
            if export_formats[i] not in [
                    ExportFormat.CHECKPOINT, ExportFormat.MODEL
            ]:
                raise TuneError("Unsupported export format: " +
                                export_formats[i])


class Trial(object):
    """A trial object holds the state for one model training run.

    Trials are themselves managed by the TrialRunner class, which implements
    the event loop for submitting trial runs to a Ray cluster.

    Trials start in the PENDING state, and transition to RUNNING once started.
    On error it transitions to ERROR, otherwise TERMINATED on success.
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    TERMINATED = "TERMINATED"
    ERROR = "ERROR"

    def __init__(self,
                 trainable_name,
                 config=None,
                 trial_id=None,
                 local_dir=DEFAULT_RESULTS_DIR,
                 evaluated_params=None,
                 experiment_tag="",
                 resources=None,
                 stopping_criterion=None,
                 checkpoint_freq=0,
                 checkpoint_at_end=False,
                 sync_on_checkpoint=True,
                 keep_checkpoints_num=None,
                 checkpoint_score_attr=TRAINING_ITERATION,
                 export_formats=None,
                 restore_path=None,
                 trial_name_creator=None,
                 loggers=None,
                 sync_to_driver_fn=None,
                 max_failures=0):
        """Initialize a new trial.

        The args here take the same meaning as the command line flags defined
        in ray.tune.config_parser.
        """
        validate_trainable(trainable_name)
        # Trial config
        self.trainable_name = trainable_name
        self.trial_id = Trial.generate_id() if trial_id is None else trial_id
        self.config = config or {}
        self.local_dir = local_dir  # This remains unexpanded for syncing.

        #: Parameters that Tune varies across searches.
        self.evaluated_params = evaluated_params or {}
        self.experiment_tag = experiment_tag
        trainable_cls = self.get_trainable_cls()
        if trainable_cls and hasattr(trainable_cls,
                                     "default_resource_request"):
            default_resources = trainable_cls.default_resource_request(
                self.config)
            if default_resources:
                if resources:
                    raise ValueError(
                        "Resources for {} have been automatically set to {} "
                        "by its `default_resource_request()` method. Please "
                        "clear the `resources_per_trial` option.".format(
                            trainable_cls, default_resources))
                resources = default_resources
        self.location = Location()
        self.resources = resources or Resources(cpu=1, gpu=0)
        self.stopping_criterion = stopping_criterion or {}
        self.loggers = loggers
        self.sync_to_driver_fn = sync_to_driver_fn
        self.verbose = True
        self.max_failures = max_failures

        # Local trial state that is updated during the run
        self.last_result = {}
        self.last_update_time = -float("inf")
        self.checkpoint_freq = checkpoint_freq
        self.checkpoint_at_end = checkpoint_at_end

        # stores in memory max/min/last result for each metric by trial
        self.metric_analysis = {}

        self.sync_on_checkpoint = sync_on_checkpoint
        newest_checkpoint = Checkpoint(Checkpoint.DISK, restore_path)
        self.checkpoint_manager = CheckpointManager(keep_checkpoints_num,
                                                    checkpoint_score_attr)
        self.checkpoint_manager.newest_checkpoint = newest_checkpoint

        self.export_formats = export_formats
        self.status = Trial.PENDING
        self.runner = None
        self.last_debug = 0
        self.error_file = None
        self.error_msg = None
        self.num_failures = 0
        self.custom_trial_name = None

        self.trial_dir_schema = TrialDirSchema(str(self), self.local_dir)
        self.logdir = self.trial_dir_schema.logdir
        self.syncer = get_syncer(self.trial_dir, self.trial_dir,
                                 self.sync_to_driver_fn)

        self.result_logger = None

        # AutoML fields
        self.results = None
        self.best_result = None
        self.param_config = None
        self.extra_arg = None

        self._nonjson_fields = [
            "checkpoint",
            "loggers",
            "sync_to_driver_fn",
            "results",
            "best_result",
            "param_config",
            "extra_arg",
        ]
        if trial_name_creator:
            self.custom_trial_name = trial_name_creator(self)

    def init_logger(self):
        """Initializes the logger."""
        if not self.result_logger:
            self.trial_dir_schema.makedirs()
            self.result_logger = UnifiedLogger(
                self.config, self.logdir, trial=self, loggers=self.loggers)

    @property
    def node_ip(self):
        return self.location.hostname

    @property
    def checkpoint(self):
        return self.checkpoint_manager.newest_checkpoint

    @classmethod
    def generate_id(cls):
        return str(uuid.uuid1().hex)[:8]

    @property
    def trial_dir(self):
        return self.trial_dir_schema.root

    @property
    def remote_logdir(self):
        return self.trial_dir_schema.remote_logdir

    @property
    def checkpoint_dir(self):
        return self.trial_dir_schema.checkpoint_dir

    def update_resources(self, cpu, gpu, **kwargs):
        """EXPERIMENTAL: Updates the resource requirements.

        Should only be called when the trial is not running.

        Raises:
            ValueError if trial status is running.
        """
        if self.status is Trial.RUNNING:
            raise ValueError("Cannot update resources while Trial is running.")
        self.resources = Resources(cpu, gpu, **kwargs)

    def sync_to_new_location(self, worker_ip):
        """Updates the logger and syncer locations.

        Also pushes checkpoints to worker_ip, allowing for cross-node recovery.
        """
        self.syncer.set_worker_ip(worker_ip)
        self.syncer.sync_up_to_new_location(worker_ip)
        self.syncer.wait()
        self.set_location(Location(worker_ip))

    def set_location(self, location):
        """Sets the location of the trial."""
        self.location = location
        if self.syncer:
            self.syncer.set_worker_ip(self.location.hostname)

    def close_logger(self):
        """Closes logger."""
        if self.result_logger:
            self.result_logger.close()
            self.result_logger = None

    def write_error_log(self, error_msg):
        if error_msg and self.logdir:
            self.num_failures += 1  # may be moved to outer scope?
            self.error_file = os.path.join(self.logdir, "error.txt")
            with open(self.error_file, "a+") as f:
                f.write("Failure # {} (occurred at {})\n".format(
                    self.num_failures, date_str()))
                f.write(error_msg + "\n")
            self.error_msg = error_msg

    def should_stop(self, result):
        """Whether the given result meets this trial's stopping criteria."""

        if result.get(DONE):
            return True

        if callable(self.stopping_criterion):
            return self.stopping_criterion(self.trial_id, result)

        for criteria, stop_value in self.stopping_criterion.items():
            if criteria not in result:
                raise TuneError(
                    "Stopping criteria {} not provided in result {}.".format(
                        criteria, result))
            elif isinstance(criteria, dict):
                raise ValueError(
                    "Stopping criteria is now flattened by default. "
                    "Use forward slashes to nest values `key1/key2/key3`.")
            elif result[criteria] >= stop_value:
                return True
        return False

    def should_checkpoint(self):
        """Whether this trial is due for checkpointing."""
        result = self.last_result or {}
        if result.get(DONE) and self.checkpoint_at_end:
            return True
        return (self.checkpoint_freq and
                result.get(TRAINING_ITERATION, 0) % self.checkpoint_freq == 0)

    def has_checkpoint(self):
        return self.checkpoint.value is not None

    def clear_checkpoint(self):
        self.checkpoint.value = None

    def on_checkpoint(self, checkpoint):
        """Hook for handling checkpoints taken by the Trainable.

        Args:
            checkpoint (Checkpoint): Checkpoint taken.
        """
        if self.sync_on_checkpoint and checkpoint.storage == Checkpoint.DISK:
            # Wait for any other syncs to finish. We need to sync again after
            # this to handle checkpoints taken mid-sync.
            self.syncer.wait()
            # Force sync down and wait before tracking the new checkpoint. This
            # prevents attempts to restore from partially synced checkpoints.
            if self.syncer.sync_down(TrialDirSchema.CHECKPOINT_DIR):
                self.syncer.wait()
            else:
                logger.error(
                    "Trial %s: Checkpoint sync skipped. "
                    "This should not happen.", self)
        self.checkpoint_manager.on_checkpoint(checkpoint)

    def should_recover(self):
        """Returns whether the trial qualifies for retrying.

        This is if the trial has not failed more than max_failures. Note this
        may return true even when there is no checkpoint, either because
        `self.checkpoint_freq` is `0` or because the trial failed before
        a checkpoint has been made.
        """
        return self.num_failures < self.max_failures or self.max_failures < 0

    def update_last_result(self, result, terminate=False):
        result.update(trial_id=self.trial_id, done=terminate)
        if self.experiment_tag:
            result.update(experiment_tag=self.experiment_tag)
        if self.verbose and (terminate or time.time() - self.last_debug >
                             DEBUG_PRINT_INTERVAL):
            print("Result for {}:".format(self))
            print("  {}".format(pretty_print(result).replace("\n", "\n  ")))
            self.last_debug = time.time()
        self.set_location(Location(result.get("node_ip"), result.get("pid")))
        self.last_result = result
        self.last_update_time = time.time()
        self.result_logger.on_result(self.last_result)
        self.syncer.sync_down_if_needed(TrialDirSchema.REMOTE_LOGDIR)
        for metric, value in flatten_dict(result).items():
            if isinstance(value, Number):
                if metric not in self.metric_analysis:
                    self.metric_analysis[metric] = {
                        "max": value,
                        "min": value,
                        "last": value
                    }
                else:
                    self.metric_analysis[metric]["max"] = max(
                        value, self.metric_analysis[metric]["max"])
                    self.metric_analysis[metric]["min"] = min(
                        value, self.metric_analysis[metric]["min"])
                    self.metric_analysis[metric]["last"] = value

    def get_trainable_cls(self):
        return get_trainable_cls(self.trainable_name)

    def set_verbose(self, verbose):
        self.verbose = verbose

    def is_finished(self):
        return self.status in [Trial.TERMINATED, Trial.ERROR]

    def __repr__(self):
        return str(self)

    def __str__(self):
        """Combines ``env`` with ``trainable_name`` and ``trial_id``.

        Can be overriden with a custom string creator.
        """
        if self.custom_trial_name:
            return self.custom_trial_name

        if "env" in self.config:
            env = self.config["env"]
            if isinstance(env, type):
                env = env.__name__
            identifier = "{}_{}".format(self.trainable_name, env)
        else:
            identifier = self.trainable_name
        identifier += "_" + self.trial_id
        return identifier.replace("/", "_")

    def __getstate__(self):
        """Memento generator for Trial.

        Sets RUNNING trials to PENDING, and flushes the result logger.
        Note this can only occur if the trial holds a DISK checkpoint.
        """
        assert self.checkpoint.storage == Checkpoint.DISK, (
            "Checkpoint must not be in-memory.")
        state = self.__dict__.copy()
        state["resources"] = resources_to_json(self.resources)

        for key in self._nonjson_fields:
            state[key] = binary_to_hex(cloudpickle.dumps(state.get(key)))

        state["runner"] = None
        state["result_logger"] = None
        if self.result_logger:
            self.result_logger.flush()
            self.syncer.sync_down()
            state["__logger_started__"] = True
        else:
            state["__logger_started__"] = False
        return copy.deepcopy(state)

    def __setstate__(self, state):
        logger_started = state.pop("__logger_started__")
        state["resources"] = json_to_resources(state["resources"])
        if state["status"] == Trial.RUNNING:
            state["status"] = Trial.PENDING
        for key in self._nonjson_fields:
            state[key] = cloudpickle.loads(hex_to_binary(state[key]))

        self.__dict__.update(state)
        validate_trainable(self.trainable_name)
        if logger_started:
            self.init_logger()
