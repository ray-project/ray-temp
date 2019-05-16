from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import torch

import ray

from ray.experimental.sgd.pytorch.pytorch_runner import PyTorchRunner
from ray.experimental.sgd.pytorch import utils


class PyTorchDistributedSGD(object):
    """Train a model using distributed PyTorch.

    Args:
        model_creator (dict -> torch.nn.Module): creates the model using the
            config.
        data_creator (dict -> Dataset, Dataset): creates the training and
            validation data sets using the config.
        optimizer_creator (model, dict -> loss, optimizer): creates the loss
            and optimizer using the config.
        config (dict): configuration passed to 'model_creator', 'data_creator',
            and 'optimizer_creator'.
        batch_size (int): batch size used for SGD.
        backend (string): backend used for distributed SGD. "gloo" or "nccl".
        mixed_precision (bool): ???
    """

    def __init__(
            self,
            model_creator,
            data_creator,
            optimizer_creator=None,  # SGD as default
            config=None,
            num_replicas=1,
            resources_per_replica=None,
            callbacks=None,  # [ray.train.Callback] TODO: support this
            batch_size=16,
            backend="gloo",
            mixed_precision=True):  # TODO: support this

        self.model_creator = model_creator
        self.config = {} if config is None else config
        self.optimizer_timer = utils.TimerStat(window_size=1)

        if resources_per_replica is None:
            resources_per_replica = utils.Resources(num_cpus=0,
                                                    num_gpus=0,
                                                    resources={})

        Runner = ray.remote(
            num_cpus=resources_per_replica.num_cpus,
            num_gpus=resources_per_replica.num_gpus,
            resources=resources_per_replica.resources)(PyTorchRunner)

        self.workers = [
            Runner.remote(model_creator, data_creator, optimizer_creator,
                          self.config, batch_size, backend)
        ]

        ip = ray.get(self.workers[0].get_node_ip.remote())
        port = int(4000 + np.random.choice(np.r_[:4000]))
        address = "tcp://{ip}:{port}".format(ip=ip, port=port)
        for i, worker in enumerate(self.workers):
            worker.setup.remote(address, i, len(self.workers))

    def train(self):
        with self.optimizer_timer:
            worker_stats = ray.get([w.step.remote() for w in self.workers])
        return worker_stats[0]  # TODO: merge worker stats

    def get_model(self):
        model = self.model_creator(self.config)
        state = ray.get(self.workers[0].get_state.remote())

        # Remove module. prefix added by distrbuted pytorch
        state = {k.replace("module.", ""): v for k, v in state.items()}

        model.load_state_dict(state)
        return model

    def save(self, ckpt):
        state = ray.get(self.workers[0].get_state.remote())
        torch.save(state, ckpt)

    def restore(self, ckpt):
        state = torch.load(ckpt)
        state_id = ray.put(state)
        ray.get([worker.set_state.remote(state_id) for worker in self.workers])

    def shutdown(self):
        for worker in self.workers:
            worker.shutdown.remote()
            worker.__ray_terminate__.remote()
