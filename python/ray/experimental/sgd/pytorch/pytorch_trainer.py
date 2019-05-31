from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import sys
import torch

import ray

from ray.experimental.sgd.pytorch.pytorch_runner import PyTorchRunner
from ray.experimental.sgd.pytorch import utils


class PyTorchTrainer(object):
    """Train a PyTorch model using distributed PyTorch.

    Launches a set of actors which connect via distributed PyTorch and
    coordinate gradient updates to train the provided model.
    """

    def __init__(self,
                 model_creator,
                 data_creator,
                 optimizer_creator=utils.sgd_mse_optimizer,
                 config=None,
                 num_replicas=1,
                 resources_per_replica=None,
                 batch_size=16,
                 backend="auto"):
        """Sets up the PyTorch trainer.

        Args:
            model_creator (dict -> torch.nn.Module): creates the model
                using the config.
            data_creator (dict -> Dataset, Dataset): creates the training
                and validation data sets using the config.
            optimizer_creator (torch.nn.Module, dict -> loss, optimizer):
                creates the loss and optimizer using the model and the config.
            config (dict): configuration passed to 'model_creator',
                'data_creator', and 'optimizer_creator'.
            num_replicas (int): the number of workers used in distributed
                training.
            resources_per_replica (Resources): resources used by each worker.
                Defaults to Resources(num_cpus=1).
            batch_size (int): batch size for an update.
            backend (string): backend used by distributed PyTorch.
        """
        # TODO: add support for mixed precision
        # TODO: add support for callbacks
        if sys.platform == "darwin":
            raise Exception(
                ("Distributed PyTorch is not supported on macOS. For more "
                 "information, see "
                 "https://github.com/pytorch/examples/issues/467."))

        self.model_creator = model_creator
        self.config = {} if config is None else config
        self.optimizer_timer = utils.TimerStat(window_size=1)

        if resources_per_replica is None:
            resources_per_replica = utils.Resources(
                num_cpus=1, num_gpus=0, resources={})

        if backend == "auto":
            backend = "nccl" if resources_per_replica.num_gpus > 0 else "gloo"

        Runner = ray.remote(
            num_cpus=resources_per_replica.num_cpus,
            num_gpus=resources_per_replica.num_gpus,
            resources=resources_per_replica.resources)(PyTorchRunner)

        def calc_batch_size(i):
            if i < batch_size % num_replicas:
                return batch_size // num_replicas + 1
            return batch_size // num_replicas

        self.workers = [
            Runner.remote(model_creator, data_creator, optimizer_creator,
                          self.config, calc_batch_size(i), backend)
            for i in range(num_replicas)
        ]

        ip = ray.get(self.workers[0].get_node_ip.remote())
        port = utils.find_free_port()
        address = "tcp://{ip}:{port}".format(ip=ip, port=port)

        # Get setup tasks in order to throw errors on failure
        ray.get([
            worker.setup.remote(address, i, len(self.workers))
            for i, worker in enumerate(self.workers)
        ])

    def train(self):
        """Runs a training epoch"""
        with self.optimizer_timer:
            worker_stats = ray.get([w.step.remote() for w in self.workers])

        train_stats = worker_stats[0].copy()
        train_stats["train_loss"] = np.mean(
            [s["train_loss"] for s in worker_stats])
        return train_stats

    def validate(self):
        """Evaluates the model on the validation data set"""
        worker_stats = ray.get([w.validate.remote() for w in self.workers])
        validation_stats = worker_stats[0].copy()
        validation_stats["validation_loss"] = np.mean(
            [s["validation_loss"] for s in worker_stats])
        return validation_stats

    def get_model(self):
        """Returns the learned model"""
        model = self.model_creator(self.config)
        state = ray.get(self.workers[0].get_state.remote())

        # Remove module. prefix added by distrbuted pytorch
        state_dict = {
            k.replace("module.", ""): v
            for k, v in state["model"].items()
        }

        model.load_state_dict(state_dict)
        return model

    def save(self, ckpt):
        """Saves the model at the provided checkpoint"""
        state = ray.get(self.workers[0].get_state.remote())
        torch.save(state, ckpt)

    def restore(self, ckpt):
        """Restores the model from the provided checkpoint"""
        state = torch.load(ckpt)
        state_id = ray.put(state)
        ray.get([worker.set_state.remote(state_id) for worker in self.workers])

    def shutdown(self):
        """Shuts down workers and releases resources"""
        for worker in self.workers:
            worker.shutdown.remote()
            worker.__ray_terminate__.remote()
