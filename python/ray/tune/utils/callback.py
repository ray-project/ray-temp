from typing import List, Optional, Type, Union

import logging
import os
import yaml

from ray.tune.callback import Callback
from ray.tune.integration.docker import DockerSyncer
from ray.tune.integration.kubernetes import NamespacedKubernetesSyncer
from ray.tune.progress_reporter import TrialProgressCallback
from ray.tune.syncer import SyncConfig
from ray.tune.logger import CSVLoggerCallback, CSVLogger, LoggerCallback, \
    JsonLoggerCallback, JsonLogger, LegacyLoggerCallback, Logger, \
    TBXLoggerCallback, TBXLogger
from ray.tune.syncer import SyncerCallback

logger = logging.getLogger(__name__)


def create_default_callbacks(callbacks: Optional[List[Callback]],
                             sync_config: SyncConfig,
                             loggers: Optional[List[Logger]],
                             metric: Optional[str] = None):
    """Create default callbacks for `tune.run()`.

    This function takes a list of existing callbacks and adds default
    callbacks to it.

    Specifically, three kinds of callbacks will be added:

    1. Loggers. Ray Tune's experiment analysis relies on CSV and JSON logging.
    2. Syncer. Ray Tune synchronizes logs and checkpoint between workers and
       the head node.
    2. Trial progress reporter. For reporting intermediate progress, like trial
       results, Ray Tune uses a callback.

    These callbacks will only be added if they don't already exist, i.e. if
    they haven't been passed (and configured) by the user. A notable case
    is when a Logger is passed, which is not a CSV or JSON logger - then
    a CSV and JSON logger will still be created.

    Lastly, this function will ensure that the Syncer callback comes after all
    Logger callbacks, to ensure that the most up-to-date logs and checkpoints
    are synced across nodes.

    """
    callbacks = callbacks or []
    has_syncer_callback = False
    has_csv_logger = False
    has_json_logger = False
    has_tbx_logger = False

    has_trial_progress_callback = any(
        isinstance(c, TrialProgressCallback) for c in callbacks)

    if not has_trial_progress_callback:
        trial_progress_callback = TrialProgressCallback(metric=metric)
        callbacks.append(trial_progress_callback)

    # Track syncer obj/index to move callback after loggers
    last_logger_index = None
    syncer_index = None

    # Create LegacyLoggerCallback for passed Logger classes
    if loggers:
        # Todo(krfricke): Deprecate `loggers` argument, print warning here.
        # Add warning as soon as we ported all loggers to LoggerCallback
        # classes.
        add_loggers = []
        for trial_logger in loggers:
            if isinstance(trial_logger, LoggerCallback):
                callbacks.append(trial_logger)
            elif isinstance(trial_logger, type) and issubclass(
                    trial_logger, Logger):
                add_loggers.append(trial_logger)
            else:
                raise ValueError(
                    f"Invalid value passed to `loggers` argument of "
                    f"`tune.run()`: {trial_logger}")
        if add_loggers:
            callbacks.append(LegacyLoggerCallback(add_loggers))

    # Check if we have a CSV, JSON and TensorboardX logger
    for i, callback in enumerate(callbacks):
        if isinstance(callback, LegacyLoggerCallback):
            last_logger_index = i
            if CSVLogger in callback.logger_classes:
                has_csv_logger = True
            if JsonLogger in callback.logger_classes:
                has_json_logger = True
            if TBXLogger in callback.logger_classes:
                has_tbx_logger = True
        elif isinstance(callback, CSVLoggerCallback):
            has_csv_logger = True
            last_logger_index = i
        elif isinstance(callback, JsonLoggerCallback):
            has_json_logger = True
            last_logger_index = i
        elif isinstance(callback, TBXLoggerCallback):
            has_tbx_logger = True
            last_logger_index = i
        elif isinstance(callback, SyncerCallback):
            syncer_index = i
            has_syncer_callback = True

    # If CSV, JSON or TensorboardX loggers are missing, add
    if os.environ.get("TUNE_DISABLE_AUTO_CALLBACK_LOGGERS", "0") != "1":
        if not has_csv_logger:
            callbacks.append(CSVLoggerCallback())
            last_logger_index = len(callbacks) - 1
        if not has_json_logger:
            callbacks.append(JsonLoggerCallback())
            last_logger_index = len(callbacks) - 1
        if not has_tbx_logger:
            callbacks.append(TBXLoggerCallback())
            last_logger_index = len(callbacks) - 1

    # If no SyncerCallback was found, add
    if not has_syncer_callback and os.environ.get(
            "TUNE_DISABLE_AUTO_CALLBACK_SYNCER", "0") != "1":

        # Detect Docker and Kubernetes environments
        _sync_to_driver = _detect_sync_to_driver(sync_config.sync_to_driver)

        syncer_callback = SyncerCallback(sync_function=_sync_to_driver)
        callbacks.append(syncer_callback)
        syncer_index = len(callbacks) - 1

    if syncer_index is not None and last_logger_index is not None and \
       syncer_index < last_logger_index:
        if (not has_csv_logger or not has_json_logger or not has_tbx_logger) \
           and not loggers:
            # Only raise the warning if the loggers were passed by the user.
            # (I.e. don't warn if this was automatic behavior and they only
            # passed a customer SyncerCallback).
            raise ValueError(
                "The `SyncerCallback` you passed to `tune.run()` came before "
                "at least one `LoggerCallback`. Syncing should be done "
                "after writing logs. Please re-order the callbacks so that "
                "the `SyncerCallback` comes after any `LoggerCallback`.")
        else:
            # If these loggers were automatically created. just re-order
            # the callbacks
            syncer_obj = callbacks[syncer_index]
            callbacks.pop(syncer_index)
            callbacks.insert(last_logger_index, syncer_obj)

    return callbacks


def _detect_sync_to_driver(
        sync_to_driver: Union[None, bool, Type],
        cluster_config_file: str = "~/ray_bootstrap_config.yaml"):
    if isinstance(sync_to_driver, Type):
        return sync_to_driver
    elif isinstance(sync_to_driver, bool) and sync_to_driver is False:
        return sync_to_driver

    # Else: True or None. Auto-detect.
    cluster_config_file = os.path.expanduser(cluster_config_file)
    if not os.path.exists(cluster_config_file):
        return sync_to_driver

    with open(cluster_config_file, "rt") as fp:
        config = yaml.safe_load(fp.read())

    if config.get("docker", None):
        return DockerSyncer

    if config.get("provider", {}).get("type", "") == "kubernetes":
        namespace = config["provider"].get("namespace", "ray")
        return NamespacedKubernetesSyncer(namespace)

    return sync_to_driver
