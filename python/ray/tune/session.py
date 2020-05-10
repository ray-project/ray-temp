import logging

logger = logging.getLogger(__name__)

_session = None


class _ReporterSession:
    def __init__(self, tune_reporter):
        self.tune_reporter = tune_reporter

    def on_result(self, **metrics):
        return self.tune_reporter(**metrics)

    @property
    def logdir(self):
        """Trial logdir (subdir of given experiment directory)"""
        return self.tune_reporter.logdir

    @property
    def trial_name(self):
        """Trial name for the corresponding trial of this Trainable"""
        return self.tune_reporter.trial_name

    @property
    def trial_id(self):
        """Trial id for the corresponding trial of this Trainable"""
        return self.tune_reporter.trial_id


def get_session():
    global _session
    if not _session:
        raise ValueError("Session not detected. Are you running Tune?")
    return _session


def init(reporter, ignore_reinit_error=True):
    """Initializes the global trial context for this process."""
    global _session

    if _session:
        # TODO(ng): would be nice to stack crawl at creation time to report
        # where that initial trial was created, and that creation line
        # info is helpful to keep around anyway.
        reinit_msg = "A session already exists in the current context."
        if ignore_reinit_error:
            if not _session.is_tune_session:
                logger.warning(reinit_msg)
            return
        else:
            raise ValueError(reinit_msg)

    _session = _ReporterSession(reporter)


def shutdown():
    """Cleans up the trial and removes it from the global context."""

    global _session
    del _session


def report(**kwargs):
    """Logs all keyword arguments.

    .. code-block:: python

        import time
        from ray import tune

        def run_me(config):
            for iter in range(100):
                time.sleep(1)
                tune.report(hello="world", ray="tune")

        analysis = tune.run(run_me)

    Args:
        **kwargs: Any key value pair to be logged by Tune. Any of these
            metrics can be used for early stopping or optimization.
    """
    _session = get_session()
    return _session.log(**kwargs)


def get_trial_dir():
    """Returns the directory where trial results are saved.

    This includes json data containing the session's parameters and metrics.
    """
    _session = get_session()
    return _session.logdir


def get_trial_name():
    """Trial name for the corresponding trial of this Trainable.

    This is not set if not using Tune.
    """
    _session = get_session()
    return _session.trial_name


def get_trial_id():
    """Trial id for the corresponding trial of this Trainable.

    This is not set if not using Tune.
    """
    _session = get_session()
    return _session.trial_id


__all__ = ["report", "get_trial_dir", "get_trial_name", "get_trial_id"]
