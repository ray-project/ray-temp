import os
import logging
from filelock import FileLock

import ray
from ray import tune
from ray.tune.resources import Resources
from ray.tune.trainable import TrainableUtil
from ray.tune.result import RESULT_DUPLICATE
from ray.tune.logger import NoopLogger

from ray.tune.function_runner import wrap_function
from horovod.ray import RayExecutor

logger = logging.getLogger(__name__)


def get_rank():
    return os.environ["HOROVOD_RANK"]


def logger_creator(log_config, logdir):
    index = get_rank()
    worker_dir = os.path.join(logdir, "worker_{}".format(index))
    os.makedirs(worker_dir, exist_ok=True)
    return NoopLogger(log_config, worker_dir)


class _HorovodTrainable(tune.Trainable):
    """Abstract Trainable class for Horovod."""
    # Callable function for training.
    _function = None
    # Number of nodes (hosts) to allocate per trial
    _num_nodes: int = 1
    _num_workers_per_node: int = 1
    _num_cpus_per_worker: int = 1
    _use_gpu: bool = False
    # bool: Whether a the function has completed training
    _finished: bool = False
    _ssh_str: str = None
    _ssh_identity_file: str = None
    _timeout_s: int = 30

    @property
    def num_workers(self):
        return self._num_nodes * self._num_workers_per_node

    def setup(self, config):
        trainable = wrap_function(self.__class__._function)
        # We use a filelock here to ensure that the file-writing
        # process is safe across different trainables.
        if self._ssh_identity_file:
            with FileLock(self._ssh_identity_file + ".lock"):
                settings = RayExecutor.create_settings(
                    self._timeout_s, self._ssh_identity_file, self._ssh_str)
        else:
            settings = RayExecutor.create_settings(
                self._timeout_s, self._ssh_identity_file, self._ssh_str)

        self.executor = RayExecutor(
            settings,
            cpus_per_slot=self._num_cpus_per_worker,
            use_gpu=self._use_gpu,
            num_hosts=self._num_nodes,
            num_slots=self._num_workers_per_node)

        # We can't put `self` in the lambda closure, so we
        # resolve the variable ahead of time.
        logdir_ = str(self.logdir)

        # Starts the workers as specified by the resources above.
        self.executor.start(
            executable_cls=trainable,
            executable_kwargs={
                "config": config,
                "logger_creator": lambda cfg: logger_creator(cfg, logdir_)
            })

    def step(self):
        if self._finished:
            raise RuntimeError("Training has already finished.")
        result = self.executor.execute(lambda w: w.step())[0]
        if RESULT_DUPLICATE in result:
            self._finished = True
        return result

    def save_checkpoint(self, checkpoint_dir):
        # TODO: optimize if colocated
        save_obj = self.executor.execute_single(lambda w: w.save_to_object())
        checkpoint_path = TrainableUtil.create_from_pickle(
            save_obj, checkpoint_dir)
        return checkpoint_path

    def load_checkpoint(self, checkpoint_dir):
        checkpoint_obj = TrainableUtil.checkpoint_to_object(checkpoint_dir)
        x_id = ray.put(checkpoint_obj)
        return self.executor.execute(lambda w: w.restore_from_object(x_id))

    def stop(self):
        self.executor.execute(lambda w: w.stop())
        self.executor.shutdown()


def DistributedTrainableCreator(func,
                                use_gpu=False,
                                num_nodes=1,
                                num_workers_per_node=1,
                                num_cpus_per_worker=1,
                                timeout_s=30,
                                replicate_pem=False):
    """Horovod Tune Converter.

    Leverages the Horovod + Ray "RayExecutor" underneath the hood.
    Requires horovod > 0.19 to work.

    Each Horovod Trainable (trial) can itself be a distributed training job.
    One basic assumption of this model is that all sub-workers
    of a trial will be placed evenly across different machines.

    It is recommended that if num_nodes per trial > 1, you set
    num_workers_per_node == the size (or number of GPUs) of a single node.
    If num_nodes == 1, then you can set num_workers_per_node to be <=
    the size (number of GPUs) of a single node.

    This above assumption can be relaxed - please file a feature request
    on Github to inform the maintainers.

    Another assumption is that this API requires gloo as the underlying
    communication primitive. You will need to install Horovod with
    `HOROVOD_WITH_GLOO` enabled.

    Fault Tolerance: The trial workers themselves are not fault tolerant.
    When a node of a trial fails, all workers of a trial are expected to
    die, and the trial is expected to restart. Checkpointing
    is TBD.

    Args:
        func (Callable[[dict], None]): A training function that takes in
            a config dict for hyperparameters and should initialize
            horovod via horovod.init.
        use_gpu (bool); Whether to allocate a GPU per worker.
        num_cpus_per_worker (int): Number of CPUs to request
            from Ray per worker.
        num_nodes (int): Number of nodes that each trial is expected
            to use.
        num_workers_per_node (int): Number of workers to start on each node.
        timeout_s (int): Seconds for Horovod rendezvous to timeout.
        replicate_pem (bool): THIS MAY BE INSECURE. If true, this will
            replicate the underlying Ray cluster ssh key across all nodes.
            This may be useful if using the Ray Autoscaler.


    Returns:
        Trainable class that can be passed into `tune.run`.

    Example:

    .. code-block::

        def train(config):
            horovod.init()
            horovod.allreduce()

        from ray.tune.integration.horovod import DistributedTrainableCreator
        trainable_cls = DistributedTrainableCreator(
            train, num_nodes=1, num_workers_per_node=2, use_gpu=True)

        tune.run(trainable_cls)


    Notes:
        This currently does not support function checkpointing.
    """
    ssh_identity_file = None
    sshkeystr = None

    if replicate_pem:
        from ray.tune.cluster_info import get_ssh_key
        ssh_identity_file = get_ssh_key()
        if os.path.exists(ssh_identity_file):
            # For now, we assume that you're on a Ray cluster.
            with open(ssh_identity_file) as f:
                sshkeystr = f.read()

    class WrappedHorovodTrainable(_HorovodTrainable):
        _function = func
        _num_nodes = num_nodes
        _num_workers_per_node = num_workers_per_node
        _num_cpus_per_worker = num_cpus_per_worker
        _use_gpu = use_gpu
        _ssh_identity_file = ssh_identity_file
        _ssh_str = sshkeystr
        _timeout_s = timeout_s

        @classmethod
        def default_resource_request(cls, config):
            extra_gpu = int(num_nodes * num_workers_per_node) * int(use_gpu)
            extra_cpu = int(
                num_nodes * num_workers_per_node * num_cpus_per_worker)

            return Resources(
                cpu=0,
                gpu=0,
                extra_cpu=extra_cpu,
                extra_gpu=extra_gpu,
            )

    return WrappedHorovodTrainable


def _train_simple(config):
    import horovod.torch as hvd
    hvd.init()
    from ray import tune
    for i in range(config.get("epochs", 2)):
        import time
        time.sleep(1)
        tune.report(test=1, rank=hvd.rank())
