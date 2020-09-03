import numpy as np
import logging
import os
import numbers
import tempfile
import torch
import torch.distributed as dist

import ray
from ray.tune import Trainable
from ray.tune.resources import Resources
from ray.tune.utils.util import merge_dicts
from ray.util.sgd.torch.worker_group import LocalWorkerGroup, RemoteWorkerGroup
from ray.util.sgd.utils import NUM_SAMPLES, BATCH_SIZE
from ray.util.sgd.torch.constants import VALID_SCHEDULER_STEP, NCCL_TIMEOUT_S
from ray.util.sgd.data import Dataset

logger = logging.getLogger(__name__)


def _validate_scheduler_step_freq(scheduler_step_freq):
    """This validation check only happens if a scheduler is passed in."""
    if scheduler_step_freq not in VALID_SCHEDULER_STEP:
        raise ValueError("Scheduler step freq must be in {}. Got {}".format(
            VALID_SCHEDULER_STEP, scheduler_step_freq))


def _remind_gpu_usage(use_gpu):
    if not use_gpu and torch.cuda.is_available():
        logger.info("GPUs detected but not using them. Set `use_gpu` to "
                    "enable GPU usage. ")


class TorchTrainer:
    """Train a PyTorch model using distributed PyTorch.

    Launches a set of actors which connect via distributed PyTorch and
    coordinate gradient updates to train the provided model. If Ray is not
    initialized, TorchTrainer will automatically initialize a local Ray
    cluster for you. Be sure to run `ray.init(address="auto")` to leverage
    multi-node training.

    .. code-block:: python

        def model_creator(config):
            return nn.Linear(1, 1)


        def optimizer_creator(model, config):
            return torch.optim.SGD(
                model.parameters(), lr=config.get("lr", 1e-4))


        def data_creator(config):
            batch_size = config["batch_size"]
            train_data, val_data = LinearDataset(2, 5), LinearDataset(2, 5)
            train_loader = DataLoader(train_data, batch_size=batch_size)
            val_loader = DataLoader(val_data, batch_size=batch_size)
            return train_loader, val_loader


        trainer = TorchTrainer(
            model_creator=model_creator,
            data_creator=data_creator,
            optimizer_creator=optimizer_creator,
            loss_creator=nn.MSELoss,
            config={"batch_size": 32},
            use_gpu=True
        )
        for i in range(4):
            trainer.train()

    The creator functions will execute before distributed coordination and
    training is setup. This is so that creator functions that download
    large datasets will not trigger any timeouts.

    The order of operations for creator functions are:

    ``data_creator`` -> ``model_creator`` -> ``optimizer_creator`` ->
    ``scheduler_creator`` -> ``loss_creator``.

    Args:
        model_creator (dict -> Model(s)): Constructor function that takes in
            config and returns the model(s) to be optimized. These must be
            ``torch.nn.Module`` objects. If multiple models are returned,
            a ``training_operator_cls`` must be specified. You do not need to
            handle GPU/devices in this function; RaySGD will do that under
            the hood.
        data_creator (dict -> Iterable(s)): Constructor function
            that takes in the passed config and returns one or
            two Iterable objects. Note that even though two Iterable objects
            can be returned, only one will be used for training, and the
            other will be used for validation. If not provided, you must
            provide a custom TrainingOperator.
        optimizer_creator ((models, dict) -> optimizers): Constructor
            function that takes in the return values from
            ``model_creator`` and the passed config and returns One or
            more Torch optimizer objects. You do not need to handle
            GPU/devices in this function; ``RaySGD`` will do that for you.
        loss_creator (torch.nn.*Loss class | dict -> loss): A constructor
            function for the training loss. This can be either a function that
            takes in the provided config for customization or a subclass
            of ``torch.nn.modules.loss._Loss``, which is most Pytorch
            loss classes. For example, ``loss_creator=torch.nn.BCELoss``.
            If not provided, you must provide a custom TrainingOperator.
        scheduler_creator ((optimizers, dict) -> scheduler):
            A constructor function for the torch scheduler. This is
            a function that takes in the generated optimizers (from
            ``optimizer_creator``) provided config for customization.
            Be sure to set ``scheduler_step_freq`` to increment the
            scheduler correctly.
        training_operator_cls (type): Custom training operator class
            that subclasses the TrainingOperator class. This class
            will be copied onto all remote workers and used to specify
            custom training and validation operations. Defaults to
            TrainingOperator.
        config (dict): Custom configuration value to be passed to
            all creator and operator constructors.
        num_workers (int): the number of workers used in distributed
            training. If 1, the worker will not be wrapped with
            DistributedDataParallel.
        num_cpus_per_worker (int): Sets the cpu requirement for each worker.
        use_gpu (bool): Sets resource allocation for workers to 1 GPU
            if true, and automatically moves both the model and optimizer
            to the available CUDA device.
        backend (string): backend used by distributed PyTorch. Currently
            support "nccl", "gloo", and "auto". If "auto", RaySGD will
            automatically use "nccl" if `use_gpu` is True, and "gloo"
            otherwise.
        serialize_data_creation (bool): A filelock will be used
            to ensure no race conditions in data downloading among
            different workers on the same node (using the local file system).
            Defaults to True.
        wrap_ddp (bool): Whether to automatically wrap DistributedDataParallel
            over each model. If False, you are expected to call it yourself.
        timeout_s (float): Seconds before the torch process group
            times out. Useful when machines are unreliable.
        add_dist_sampler (bool): Whether to automatically add a
            DistributedSampler to all created dataloaders. Only applicable
            if num_workers > 1.
        use_fp16 (bool): Enables mixed precision training via apex if apex
            is installed. This is automatically done after the model and
            optimizers are constructed and will work for multi-model training.
            Please see https://github.com/NVIDIA/apex for more details.
        apex_args (dict|None): Dict containing keyword args for amp.initialize.
            See https://nvidia.github.io/apex/amp.html#module-apex.amp. By
            default, the models and optimizers are passed in. Consider using
            "num_losses" if operating over multiple models and optimizers.
        scheduler_step_freq: "batch", "epoch", "manual", or None. This will
            determine when ``scheduler.step`` is called. If "batch",
            ``step`` will be called after every optimizer step. If "epoch",
            ``step`` will be called after one pass of the DataLoader. If
            "manual", the scheduler will not be incremented automatically -
            you are expected to call ``trainer.update_schedulers`` manually.
            If a scheduler is passed in, this value is expected to not be None.
        use_local (bool): If True, 1 worker will be a local worker running
            on the driver process, and all other workers will be remote. If
            False, all workers will be remote. Set this to True for easy
            debugging of worker on driver process. Defaults to False.

    """

    # TODO: Implement autoscaling. If num_workers=-1, the trainer will use as
    # many resources as available. Upon each train call, TorchTrainer will
    # query the Ray global state for total available resources and resize
    # its remote workers to consume all available resources.

    def __init__(
            self,
            *,
            model_creator,
            data_creator,
            optimizer_creator,
            loss_creator=None,
            scheduler_creator=None,
            training_operator_cls=None,
            initialization_hook=None,
            config=None,
            num_workers=1,
            num_cpus_per_worker=1,
            use_gpu="auto",
            backend="auto",
            wrap_ddp=True,
            timeout_s=NCCL_TIMEOUT_S,
            serialize_data_creation=True,
            use_fp16=False,
            use_tqdm=False,
            apex_args=None,
            add_dist_sampler=True,
            scheduler_step_freq=None,
            num_replicas=None,
            batch_size=None,
            data_loader_args=None,
            use_local=False,
    ):
        if num_workers > 1 and not dist.is_available():
            raise ValueError(
                ("Distributed PyTorch is not supported on macOS. "
                 "To run without distributed PyTorch, set 'num_workers=1'. "
                 "For more information, see "
                 "https://github.com/pytorch/examples/issues/467."))

        if not (callable(model_creator) and callable(optimizer_creator)):
            raise ValueError(
                "Must provide a callable model_creator and optimizer_creator.")

        if num_replicas is not None:
            raise DeprecationWarning(
                "num_replicas is deprecated. Use num_workers instead.")

        if batch_size is not None:
            raise DeprecationWarning(
                "batch_size is deprecated. Use config={'batch_size': N} "
                "specify a batch size for each worker or "
                "config={ray.util.sgd.utils.BATCH_SIZE: N} to specify a "
                "batch size to be used across all workers.")

        if data_loader_args:
            raise ValueError(
                "data_loader_args is deprecated. You can return a "
                "torch.utils.data.DataLoader in data_creator. Ray will "
                "automatically set a DistributedSampler if a DataLoader is "
                "returned and num_workers > 1.")

        self.model_creator = model_creator
        self.optimizer_creator = optimizer_creator
        self.loss_creator = loss_creator
        self.data_creator = data_creator
        self.scheduler_creator = scheduler_creator
        self.training_operator_cls = training_operator_cls

        if not training_operator_cls and not loss_creator:
            raise ValueError("If a loss_creator is not provided, you must "
                             "provide a custom training operator.")

        self.initialization_hook = initialization_hook
        self.config = {} if config is None else config
        if use_gpu == "auto":
            use_gpu = torch.cuda.is_available()

        _remind_gpu_usage(use_gpu)

        if backend == "auto":
            backend = "nccl" if use_gpu else "gloo"

        logger.debug(f"Using {backend} as backend.")
        self.backend = backend
        self.num_cpus_per_worker = num_cpus_per_worker
        self.use_gpu = use_gpu
        self.max_replicas = num_workers

        self.serialize_data_creation = serialize_data_creation
        self.wrap_ddp = wrap_ddp
        self.timeout_s = timeout_s
        self.use_fp16 = use_fp16
        self.use_tqdm = use_tqdm
        self.add_dist_sampler = add_dist_sampler

        if apex_args and not isinstance(apex_args, dict):
            raise ValueError("apex_args needs to be a dict object.")

        self.apex_args = apex_args
        self.temp_dir = tempfile.mkdtemp(prefix="raysgd")
        self._num_failures = 0
        self._last_resize = float("-inf")

        if use_local:
            self.worker_group = LocalWorkerGroup()
        else:
            self.worker_group = RemoteWorkerGroup()

        if scheduler_creator:
            _validate_scheduler_step_freq(scheduler_step_freq)

        self.scheduler_step_freq = scheduler_step_freq

        if not ray.is_initialized() and self.max_replicas > 1:
            logger.info("Automatically initializing single-node Ray. To use "
                        "multi-node training, be sure to run `ray.init("
                        "address='auto')` before instantiating the Trainer.")
            ray.init()
        self._start_workers(self.max_replicas)

    def _configure_and_split_batch(self, num_workers):
        """If sgd.utils.BATCH_SIZE is provided, split among workers."""
        if BATCH_SIZE not in self.config:
            return
        # Compute batch size per worker
        logger.debug("BATCH_SIZE parameter detected. Splitting among workers.")
        batch_size = self.config[BATCH_SIZE]
        batch_size_per_worker = batch_size // num_workers
        if batch_size % num_workers > 0:
            new_batch_size = batch_size_per_worker * num_workers
            logger.warning(
                ("Changing batch size from {old_batch_size} to "
                 "{new_batch_size} to evenly distribute batches across "
                 "{num_workers} workers.").format(
                     old_batch_size=batch_size,
                     new_batch_size=new_batch_size,
                     num_workers=num_workers))
            self.config[BATCH_SIZE] = new_batch_size
        return batch_size_per_worker

    def _start_workers(self, num_workers):
        worker_config = self.config.copy()
        batch_size_per_worker = self._configure_and_split_batch(num_workers)
        if batch_size_per_worker:
            worker_config[BATCH_SIZE] = batch_size_per_worker
        params = dict(
            model_creator=self.model_creator,
            data_creator=self.data_creator,
            optimizer_creator=self.optimizer_creator,
            loss_creator=self.loss_creator,
            scheduler_creator=self.scheduler_creator,
            training_operator_cls=self.training_operator_cls,
            config=worker_config,
            serialize_data_creation=self.serialize_data_creation,
            use_fp16=self.use_fp16,
            use_gpu=self.use_gpu,
            use_tqdm=self.use_tqdm,
            apex_args=self.apex_args,
            scheduler_step_freq=self.scheduler_step_freq)

        if num_workers > 1:
            params.update(
                backend=self.backend,
                add_dist_sampler=self.add_dist_sampler,
                wrap_ddp=self.wrap_ddp)

        self.worker_group.start_workers(
            num_workers,
            params,
            initialization_hook=self.initialization_hook,
            num_cpus_per_worker=self.num_cpus_per_worker,
            use_gpu=self.use_gpu,
            timeout_s=self.timeout_s)

    def train(self,
              num_steps=None,
              profile=False,
              reduce_results=True,
              max_retries=3,
              info=None,
              dataset=None):
        """Runs a training epoch.

        Calls `operator.train_epoch()` on N parallel workers simultaneously
        underneath the hood.

        Set `max_retries` to enable fault handling in case of
        instance preemption.

        Args:
            num_steps (int): Number of batches to compute update steps on.
                This corresponds also to the number of times
                ``TrainingOperator.train_batch`` is called.
            profile (bool): Returns time stats for the training procedure.
            reduce_results (bool): Whether to average all metrics across
                all workers into one dict. If a metric is a non-numerical
                value (or nested dictionaries), one value will be randomly
                selected among the workers. If False, returns a list of dicts.
            max_retries (int): Must be non-negative. If set to N, TorchTrainer
                will detect and recover from training failure. The recovery
                process will kill all current workers, query the Ray
                global state for total available resources, and re-launch up to
                the available resources. Behavior is not well-defined
                in case of shared cluster usage. Defaults to 3.
            info (dict): Optional dictionary passed to the training
                operator for ``train_epoch`` and ``train_batch``.
            dataset (Dataset): Optional dataset to train with. If specified,
                the dataloader passed in via data_creator will be ignored.

        Returns:
            (dict | list) A dictionary of metrics for training.
                You can provide custom metrics by passing in a custom
                ``training_operator_cls``. If ``reduce_results=False``,
                this will return a list of metric dictionaries whose
                length will be equal to ``num_workers``.
        """
        assert max_retries >= 0, "`max_retries` must be non-negative."
        assert isinstance(dataset, Dataset) is not None \
            or self.data_creator, \
            "Must specify either a data creator or a dataset"
        if self.worker_group.should_resize():
            logger.info("Resize opportunity detected. Attempting to scale up.")
            self.worker_group.resize_workers()
        success, worker_stats = self.worker_group.train(
            num_steps=num_steps, profile=profile, info=info, dataset=dataset)
        # Fault handling
        for i in range(max_retries):
            if success:
                break
            else:
                self._num_failures += 1
            self.worker_group.resize_workers()
            logger.info("Retrying training step with %d workers." %
                        (self.worker_group.get_num_workers()))
            success, worker_stats = self.worker_group.train(
                num_steps=num_steps,
                profile=profile,
                info=info,
                dataset=dataset)
        if not success:
            raise RuntimeError("Training run failed.")

        if reduce_results:
            return self._process_stats(worker_stats)
        else:
            return worker_stats

    def _process_stats(self, worker_stats):
        stats = {
            NUM_SAMPLES: sum(
                stats.pop(NUM_SAMPLES, np.nan) for stats in worker_stats)
        }

        for stat_key in worker_stats[0]:
            if isinstance(worker_stats[0], numbers.Number):
                stats[stat_key] = np.nanmean(
                    [s.get(stat_key, np.nan) for s in worker_stats])
            else:
                stats[stat_key] = worker_stats[0][stat_key]
        return stats

    def apply_all_workers(self, fn):
        """Run a function on all operators on the workers.

        Args:
            fn (Callable): A function that takes in no arguments.

        Returns:
            A list of objects returned by ``fn`` on each worker.

        """
        return self.worker_group.apply_all_workers(fn)

    def apply_all_operators(self, fn):
        """Run a function on all operators on the workers.

        Args:
            fn (Callable[TrainingOperator]): A function that takes in a
                TrainingOperator.

        Returns:
            A list of objects returned by ``fn`` on each operator.

        """
        return self.worker_group.apply_all_operators(fn)

    def validate(self,
                 num_steps=None,
                 profile=False,
                 reduce_results=True,
                 info=None):
        """Evaluates the model on the validation data set.

        Args:
            num_steps (int): Number of batches to compute update steps on.
                This corresponds also to the number of times
                ``TrainingOperator.validate_batch`` is called.
            profile (bool): Returns time stats for the evaluation procedure.
            reduce_results (bool): Whether to average all metrics across
                all workers into one dict. If a metric is a non-numerical
                value (or nested dictionaries), one value will be randomly
                selected among the workers. If False, returns a list of dicts.
            info (dict): Optional dictionary passed to the training
                operator for `validate` and `validate_batch`.

        Returns:
            A dictionary of metrics for validation.
                You can provide custom metrics by passing in a custom
                ``training_operator_cls``.
        """
        worker_stats = self.worker_group.validate(
            num_steps=num_steps, profile=profile, info=info)

        if reduce_results:
            return self._process_stats(worker_stats)
        else:
            return worker_stats

    def update_scheduler(self, metric):
        """Calls ``scheduler.step(metric)`` on all schedulers.

        This is useful for lr_schedulers such as ``ReduceLROnPlateau``.
        """
        self.worker_group.apply_all_operators(
            lambda op: [sched.step(metric) for sched in op.schedulers])

    def get_model(self):
        """Returns the learned model(s)."""
        unwrapped = []
        models = self.worker_group.get_model()
        for model in models:
            unwrapped += [model.module if hasattr(model, "module") else model]
        if len(unwrapped) == 1:
            return unwrapped[0]
        return unwrapped

    def get_local_operator(self):
        """Returns the local TrainingOperator object.

        Be careful not to perturb its state, or else you can cause the system
        to enter an inconsistent state.

        Returns:
            TrainingOperator: The local TrainingOperator object.
        """
        return self.worker_group.get_local_operator()

    def state_dict(self):
        return self.worker_group.state_dict()

    def load_state_dict(self, state_dict, blocking=False):
        self.worker_group.load_state_dict(state_dict, blocking=blocking)

    def save(self, checkpoint):
        """Saves the Trainer state to the provided checkpoint path.

        Args:
            checkpoint (str): Path to target checkpoint file.
        """
        torch.save(self.state_dict(), checkpoint)
        return checkpoint

    def load(self, checkpoint):
        """Loads the Trainer and all workers from the provided checkpoint.

        Args:
            checkpoint (str): Path to target checkpoint file.
        """
        state_dict = torch.load(checkpoint)
        self.load_state_dict(state_dict)

    def restore(self, *args):
        raise DeprecationWarning("Use `TorchTrainer.load()` instead.")

    def shutdown(self, force=False):
        """Shuts down workers and releases resources."""
        self.worker_group.shutdown(force=force)

    @classmethod
    def as_trainable(cls, *args, **kwargs):
        """Creates a BaseTorchTrainable class compatible with Tune.

        Any configuration parameters will be overriden by the Tune
        Trial configuration. You can also subclass the provided Trainable
        to implement your own iterative optimization routine.

        .. code-block:: python

            TorchTrainable = TorchTrainer.as_trainable(
                model_creator=ResNet18,
                data_creator=cifar_creator,
                optimizer_creator=optimizer_creator,
                loss_creator=nn.CrossEntropyLoss,
                num_gpus=2
            )
            analysis = tune.run(
                TorchTrainable,
                config={"lr": tune.grid_search([0.01, 0.1])}
            )

        """

        class TorchTrainable(BaseTorchTrainable):
            @classmethod
            def default_resource_request(cls, config):
                num_workers = config.get("num_workers",
                                         kwargs.get("num_workers", 1))
                num_cpus = config.get("num_cpus_per_worker",
                                      kwargs.get("num_cpus_per_worker", 1))
                use_gpu = config.get("use_gpu", kwargs.get("use_gpu"))
                use_local = config.get("use_local",
                                       kwargs.get("use_local", False))

                if use_local:
                    remote_worker_count = num_workers - 1
                else:
                    remote_worker_count = num_workers

                return Resources(
                    cpu=num_cpus,
                    gpu=int(use_gpu),
                    extra_cpu=int(remote_worker_count),
                    extra_gpu=int(int(use_gpu) * remote_worker_count))

            def _create_trainer(self, tune_config):
                """Overrides the provided config with Tune config."""
                provided_config = kwargs.get("config", {}).copy()
                provided_config.update(tune_config)
                kwargs["config"] = provided_config
                trainer = TorchTrainer(*args, **kwargs)
                return trainer

        return TorchTrainable


class BaseTorchTrainable(Trainable):
    """Base class for converting TorchTrainer to a Trainable class.

    This class is produced when you call ``TorchTrainer.as_trainable(...)``.

    You can override the produced Trainable to implement custom iterative
    training procedures:

    .. code-block:: python

        TorchTrainable = TorchTrainer.as_trainable(
            model_creator=ResNet18,
            data_creator=cifar_creator,
            optimizer_creator=optimizer_creator,
            loss_creator=nn.CrossEntropyLoss,
            num_gpus=2
        )
        # TorchTrainable is subclass of BaseTorchTrainable.

        class CustomTrainable(TorchTrainable):
            def step(self):
                for i in range(5):
                    train_stats = self.trainer.train()
                validation_stats = self.trainer.validate()
                train_stats.update(validation_stats)
                return train_stats

        analysis = tune.run(
            CustomTrainable,
            config={"lr": tune.grid_search([0.01, 0.1])}
        )

    """

    def setup(self, config):
        """Constructs a TorchTrainer object as `self.trainer`."""
        self._trainer = self._create_trainer(config)

    def step(self):
        """Calls `self.trainer.train()` and `self.trainer.validate()` once.

        You may want to override this if using a custom LR scheduler.
        """
        train_stats = self.trainer.train(max_retries=10, profile=True)
        validation_stats = self.trainer.validate(profile=True)
        stats = merge_dicts(train_stats, validation_stats)
        return stats

    def save_checkpoint(self, checkpoint_dir):
        """Returns a path containing the trainer state."""
        checkpoint_path = os.path.join(checkpoint_dir, "trainer.checkpoint")
        self.trainer.save(checkpoint_path)
        return checkpoint_path

    def load_checkpoint(self, checkpoint_path):
        """Restores the trainer state.

        Override this if you have state external to the Trainer object.
        """
        return self.trainer.load(checkpoint_path)

    def cleanup(self):
        """Shuts down the trainer."""
        self.trainer.shutdown()

    def _create_trainer(self, config):
        raise NotImplementedError

    @property
    def trainer(self):
        """An instantiated TorchTrainer object.

        Use this when specifying custom training procedures for Tune.
        """
        return self._trainer
