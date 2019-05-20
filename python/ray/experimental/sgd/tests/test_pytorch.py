from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pytest
import sys
import tempfile
import time
import torch

from ray.tests.conftest import ray_start_2_cpus
from ray.experimental.sgd.pytorch.utils import Resources
from ray.experimental.sgd.pytorch.pytorch_trainer import PyTorchTrainer

from ray.experimental.sgd.tests.pytorch_utils import (
    model_creator, optimizer_creator, data_creator)


@pytest.mark.skipif(
    sys.platform == "darwin",
    reason="Doesn't work on macOS."
)
def test_train(ray_start_2_cpus):
    trainer = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        num_replicas=2,
        resources_per_replica=Resources(num_cpus=1))
    train_loss1 = trainer.train()["train_loss"]
    validation_loss1 = trainer.validate()["validation_loss"]

    train_loss2 = trainer.train()["train_loss"]
    validation_loss2 = trainer.validate()["validation_loss"]

    print(train_loss1, train_loss2)
    print(validation_loss1, validation_loss2)

    assert train_loss2 <= train_loss1
    assert validation_loss2 <= validation_loss1


@pytest.mark.skipif(
    sys.platform == "darwin",
    reason="Doesn't work on macOS."
)
def test_save_and_restore(ray_start_2_cpus):
    trainer1 = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        num_replicas=2,
        resources_per_replica=Resources(num_cpus=1))
    trainer1.train()

    filename = os.path.join(tempfile.mkdtemp(), "checkpoint")
    trainer1.save(filename)

    model1 = trainer1.get_model()

    trainer1.shutdown()

    trainer2 = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        num_replicas=2,
        resources_per_replica=Resources(num_cpus=1))
    trainer2.restore(filename)

    os.remove(filename)

    model2 = trainer2.get_model()

    model1_state_dict = model1.state_dict()
    model2_state_dict = model2.state_dict()

    assert set(model1_state_dict.keys()) == set(model2_state_dict.keys())

    for k in model1_state_dict:
        assert torch.equal(model1_state_dict[k], model2_state_dict[k])
