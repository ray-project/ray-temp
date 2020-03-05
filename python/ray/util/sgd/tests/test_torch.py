import os
import tempfile
from unittest.mock import patch
import numpy as np
import pytest
import time
import torch
import torch.nn as nn
import torch.distributed as dist

import ray
from ray import tune
from ray.util.sgd.torch import TorchTrainer, TorchTrainable
from ray.util.sgd.torch.training_operator import (_TestingOperator,
                                                  _TestMetricsOperator)
from ray.util.sgd.torch.constants import SCHEDULER_STEP
from ray.util.sgd.utils import check_for_failure, NUM_SAMPLES, BATCH_COUNT

from ray.util.sgd.torch.examples.train_example import (
    model_creator, optimizer_creator, data_creator, LinearDataset)


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_single_step(ray_start_2_cpus):  # noqa: F811
    trainer = TorchTrainer(
        model_creator=model_creator,
        data_creator=data_creator,
        optimizer_creator=optimizer_creator,
        loss_creator=lambda config: nn.MSELoss(),
        num_workers=1)
    metrics = trainer.train(num_steps=1)
    assert metrics[BATCH_COUNT] == 1

    val_metrics = trainer.validate(num_steps=1)
    assert val_metrics[BATCH_COUNT] == 1


@pytest.mark.parametrize("num_workers", [1, 2] if dist.is_available() else [1])
def test_train(ray_start_2_cpus, num_workers):  # noqa: F811
    trainer = TorchTrainer(
        model_creator=model_creator,
        data_creator=data_creator,
        optimizer_creator=optimizer_creator,
        loss_creator=lambda config: nn.MSELoss(),
        num_workers=num_workers)
    for i in range(3):
        train_loss1 = trainer.train()["mean_train_loss"]
    validation_loss1 = trainer.validate()["mean_val_loss"]

    for i in range(3):
        train_loss2 = trainer.train()["mean_train_loss"]
    validation_loss2 = trainer.validate()["mean_val_loss"]

    assert train_loss2 <= train_loss1, (train_loss2, train_loss1)
    assert validation_loss2 <= validation_loss1, (validation_loss2,
                                                  validation_loss1)


@pytest.mark.parametrize("num_workers", [1, 2] if dist.is_available() else [1])
def test_multi_model(ray_start_2_cpus, num_workers):
    def train(*, model=None, criterion=None, optimizer=None, dataloader=None):
        model.train()
        train_loss = 0
        correct = 0
        total = 0
        for batch_idx, (inputs, targets) in enumerate(dataloader):
            optimizer.zero_grad()
            outputs = model(inputs)
            loss = criterion(outputs, targets)
            loss.backward()
            optimizer.step()

            train_loss += loss.item()
            _, predicted = outputs.max(1)
            total += targets.size(0)
            correct += predicted.eq(targets).sum().item()
        return {
            "accuracy": correct / total,
            "train_loss": train_loss / (batch_idx + 1)
        }

    def train_epoch(self, iterator, info):
        result = {}
        for i, (model, optimizer) in enumerate(
                zip(self.models, self.optimizers)):
            result["model_{}".format(i)] = train(
                model=model,
                criterion=self.criterion,
                optimizer=optimizer,
                dataloader=iterator)
        return result

    def multi_model_creator(config):
        return nn.Linear(1, 1), nn.Linear(1, 1)

    def multi_optimizer_creator(models, config):
        opts = [
            torch.optim.SGD(model.parameters(), lr=0.0001) for model in models
        ]
        return opts[0], opts[1]

    trainer1 = TorchTrainer(
        model_creator=multi_model_creator,
        data_creator=data_creator,
        optimizer_creator=multi_optimizer_creator,
        loss_creator=lambda config: nn.MSELoss(),
        config={"custom_func": train_epoch},
        training_operator_cls=_TestingOperator,
        num_workers=num_workers)
    trainer1.train()

    filename = os.path.join(tempfile.mkdtemp(), "checkpoint")
    trainer1.save(filename)

    models1 = trainer1.get_model()

    trainer1.shutdown()

    trainer2 = TorchTrainer(
        model_creator=multi_model_creator,
        data_creator=data_creator,
        optimizer_creator=multi_optimizer_creator,
        loss_creator=lambda config: nn.MSELoss(),
        config={"custom_func": train_epoch},
        training_operator_cls=_TestingOperator,
        num_workers=num_workers)
    trainer2.restore(filename)

    os.remove(filename)

    models2 = trainer2.get_model()

    for model_1, model_2 in zip(models1, models2):

        model1_state_dict = model_1.state_dict()
        model2_state_dict = model_2.state_dict()

        assert set(model1_state_dict.keys()) == set(model2_state_dict.keys())

        for k in model1_state_dict:
            assert torch.equal(model1_state_dict[k], model2_state_dict[k])

    trainer2.shutdown()


@pytest.mark.parametrize("num_workers", [1, 2] if dist.is_available() else [1])
def test_multi_model_matrix(ray_start_2_cpus, num_workers):  # noqa: F811
    def train_epoch(self, iterator, info):
        if self.config.get("models", 1) > 1:
            assert len(self.models) == self.config["models"], self.config

        if self.config.get("optimizers", 1) > 1:
            assert len(
                self.optimizers) == self.config["optimizers"], self.config

        if self.config.get("schedulers", 1) > 1:
            assert len(
                self.schedulers) == self.config["schedulers"], self.config
        return {"done": 1}

    def multi_model_creator(config):
        models = []
        for i in range(config.get("models", 1)):
            models += [nn.Linear(1, 1)]
        return models[0] if len(models) == 1 else models

    def multi_optimizer_creator(models, config):
        optimizers = []
        main_model = models[0] if type(models) is list else models
        for i in range(config.get("optimizers", 1)):
            optimizers += [torch.optim.SGD(main_model.parameters(), lr=0.0001)]
        return optimizers[0] if len(optimizers) == 1 else optimizers

    def multi_scheduler_creator(optimizer, config):
        schedulers = []
        main_opt = optimizer[0] if type(optimizer) is list else optimizer
        for i in range(config.get("schedulers", 1)):
            schedulers += [
                torch.optim.lr_scheduler.StepLR(
                    main_opt, step_size=30, gamma=0.1)
            ]
        return schedulers[0] if len(schedulers) == 1 else schedulers

    for model_count in range(1, 3):
        for optimizer_count in range(1, 3):
            for scheduler_count in range(1, 3):
                trainer = TorchTrainer(
                    model_creator=multi_model_creator,
                    data_creator=data_creator,
                    optimizer_creator=multi_optimizer_creator,
                    loss_creator=nn.MSELoss,
                    scheduler_creator=multi_scheduler_creator,
                    training_operator_cls=_TestingOperator,
                    num_workers=num_workers,
                    config={
                        "models": model_count,
                        "optimizers": optimizer_count,
                        "schedulers": scheduler_count,
                        "custom_func": train_epoch
                    })
                trainer.train()
                trainer.shutdown()


@pytest.mark.parametrize("scheduler_freq", ["epoch", "batch"])
def test_scheduler_freq(ray_start_2_cpus, scheduler_freq):  # noqa: F811
    def train_epoch(self, iterator, info):
        assert info[SCHEDULER_STEP] == scheduler_freq
        return {"done": 1}

    def scheduler_creator(optimizer, config):
        return torch.optim.lr_scheduler.StepLR(
            optimizer, step_size=30, gamma=0.1)

    trainer = TorchTrainer(
        model_creator=model_creator,
        data_creator=data_creator,
        optimizer_creator=optimizer_creator,
        loss_creator=lambda config: nn.MSELoss(),
        config={"custom_func": train_epoch},
        training_operator_cls=_TestingOperator,
        scheduler_creator=scheduler_creator,
        scheduler_step_freq=scheduler_freq)

    for i in range(3):
        trainer.train()
    trainer.shutdown()


def test_profiling(ray_start_2_cpus):  # noqa: F811
    trainer = TorchTrainer(
        model_creator=model_creator,
        data_creator=data_creator,
        optimizer_creator=optimizer_creator,
        loss_creator=lambda config: nn.MSELoss())

    stats = trainer.train(profile=True)
    assert "profile" in stats
    stats = trainer.validate(profile=True)
    assert "profile" in stats
    trainer.shutdown()


@pytest.mark.parametrize("num_workers", [1, 2] if dist.is_available() else [1])
def test_metrics(ray_start_2_cpus, num_workers):
    data_size, val_size = 600, 500
    batch_size = 4

    num_train_steps = int(data_size / batch_size)
    num_val_steps = int(val_size / batch_size)

    train_scores = [1] + ([0] * num_train_steps)
    val_scores = [1] + ([0] * num_val_steps)
    trainer = TorchTrainer(
        model_creator=model_creator,
        data_creator=data_creator,
        optimizer_creator=optimizer_creator,
        loss_creator=lambda config: nn.MSELoss(),
        num_workers=num_workers,
        config={
            "scores": train_scores,
            "val_scores": val_scores,
            "key": "score",
            "batch_size": batch_size,
            "data_size": data_size,
            "val_size": val_size
        },
        training_operator_cls=_TestMetricsOperator)

    stats = trainer.train(num_steps=num_train_steps)
    # Test that we output mean and last of custom metrics in an epoch
    assert "mean_score" in stats
    assert stats["last_score"] == 0

    assert stats[NUM_SAMPLES] == num_train_steps * batch_size
    expected_score = num_workers * (sum(train_scores) /
                                    (num_train_steps * batch_size))
    assert np.allclose(stats["mean_score"], expected_score)

    val_stats = trainer.validate()
    # Test that we output mean and last of custom metrics in validation
    assert val_stats["last_score"] == 0
    expected_score = (sum(val_scores) /
                      (num_val_steps * batch_size)) * num_workers
    assert np.allclose(val_stats["mean_score"], expected_score)
    assert val_stats[BATCH_COUNT] == np.ceil(num_val_steps / num_workers)
    assert val_stats[NUM_SAMPLES] == num_val_steps * batch_size
    assert val_stats[NUM_SAMPLES] == val_size

    trainer.shutdown()


@pytest.mark.parametrize("num_workers", [1, 2] if dist.is_available() else [1])
def test_metrics_nan(ray_start_2_cpus, num_workers):
    data_size, val_size = 100, 100
    batch_size = 10

    num_train_steps = int(data_size / batch_size)
    num_val_steps = int(val_size / batch_size)

    train_scores = [np.nan] + ([0] * num_train_steps)
    val_scores = [np.nan] + ([0] * num_val_steps)
    trainer = TorchTrainer(
        model_creator=model_creator,
        data_creator=data_creator,
        optimizer_creator=optimizer_creator,
        loss_creator=lambda config: nn.MSELoss(),
        num_workers=num_workers,
        config={
            "scores": train_scores,
            "val_scores": val_scores,
            "key": "score",
            "batch_size": batch_size,
            "data_size": data_size,
            "val_size": val_size
        },
        training_operator_cls=_TestMetricsOperator)

    stats = trainer.train(num_steps=num_train_steps)
    assert "mean_score" in stats
    assert stats["last_score"] == 0
    assert np.isnan(stats["mean_score"])

    stats = trainer.validate()
    assert "mean_score" in stats
    assert stats["last_score"] == 0
    assert np.isnan(stats["mean_score"])


def test_scheduler_validate(ray_start_2_cpus):  # noqa: F811
    from torch.optim.lr_scheduler import ReduceLROnPlateau

    trainer = TorchTrainer(
        model_creator=model_creator,
        data_creator=data_creator,
        optimizer_creator=optimizer_creator,
        loss_creator=lambda config: nn.MSELoss(),
        scheduler_creator=lambda optimizer, cfg: ReduceLROnPlateau(optimizer),
        training_operator_cls=_TestingOperator)
    trainer.update_scheduler(0.5)
    trainer.update_scheduler(0.5)
    assert all(
        trainer.apply_all_operators(
            lambda op: op.schedulers[0].last_epoch == 2))
    trainer.shutdown()


@pytest.mark.parametrize("num_workers", [1, 2] if dist.is_available() else [1])
def test_tune_train(ray_start_2_cpus, num_workers):  # noqa: F811

    config = {
        "model_creator": model_creator,
        "data_creator": data_creator,
        "optimizer_creator": optimizer_creator,
        "loss_creator": lambda config: nn.MSELoss(),
        "num_workers": num_workers,
        "use_gpu": False,
        "backend": "gloo",
        "config": {
            "batch_size": 512,
            "lr": 0.001
        }
    }

    analysis = tune.run(
        TorchTrainable,
        num_samples=2,
        config=config,
        stop={"training_iteration": 2},
        verbose=1)

    # checks loss decreasing for every trials
    for path, df in analysis.trial_dataframes.items():
        mean_train_loss1 = df.loc[0, "mean_train_loss"]
        mean_train_loss2 = df.loc[1, "mean_train_loss"]
        mean_val_loss1 = df.loc[0, "mean_val_loss"]
        mean_val_loss2 = df.loc[1, "mean_val_loss"]

        assert mean_train_loss2 <= mean_train_loss1
        assert mean_val_loss2 <= mean_val_loss1


@pytest.mark.parametrize("num_workers", [1, 2] if dist.is_available() else [1])
def test_save_and_restore(ray_start_2_cpus, num_workers):  # noqa: F811
    trainer1 = TorchTrainer(
        model_creator=model_creator,
        data_creator=data_creator,
        optimizer_creator=optimizer_creator,
        loss_creator=lambda config: nn.MSELoss(),
        num_workers=num_workers)
    trainer1.train()

    filename = os.path.join(tempfile.mkdtemp(), "checkpoint")
    trainer1.save(filename)

    model1 = trainer1.get_model()

    trainer1.shutdown()

    trainer2 = TorchTrainer(
        model_creator=model_creator,
        data_creator=data_creator,
        optimizer_creator=optimizer_creator,
        loss_creator=lambda config: nn.MSELoss(),
        num_workers=num_workers)
    trainer2.restore(filename)

    os.remove(filename)

    model2 = trainer2.get_model()

    model1_state_dict = model1.state_dict()
    model2_state_dict = model2.state_dict()

    assert set(model1_state_dict.keys()) == set(model2_state_dict.keys())

    for k in model1_state_dict:
        assert torch.equal(model1_state_dict[k], model2_state_dict[k])


def test_fail_with_recover(ray_start_2_cpus):  # noqa: F811
    if not dist.is_available():
        return

    def single_loader(config):
        dataset = LinearDataset(2, 5, size=1000000)
        return torch.utils.data.DataLoader(
            dataset, batch_size=config.get("batch_size", 32))

    def step_with_fail(self, *args, **kwargs):
        worker_stats = [
            w.train_epoch.remote(*args, **kwargs) for w in self.workers
        ]
        if self._num_failures < 3:
            time.sleep(1)  # Make the batch will fail correctly.
            self.workers[0].__ray_kill__()
        success = check_for_failure(worker_stats)
        return success, worker_stats

    with patch.object(TorchTrainer, "_train_epoch", step_with_fail):
        trainer1 = TorchTrainer(
            model_creator=model_creator,
            data_creator=single_loader,
            optimizer_creator=optimizer_creator,
            loss_creator=lambda config: nn.MSELoss(),
            config={"batch_size": 100000},
            num_workers=2)

        with pytest.raises(RuntimeError):
            trainer1.train(max_retries=1)


def test_resize(ray_start_2_cpus):  # noqa: F811
    if not dist.is_available():
        return

    def single_loader(config):
        dataset = LinearDataset(2, 5, size=1000000)
        return torch.utils.data.DataLoader(
            dataset, batch_size=config.get("batch_size", 32))

    def step_with_fail(self, *args, **kwargs):
        worker_stats = [
            w.train_epoch.remote(*args, **kwargs) for w in self.workers
        ]
        if self._num_failures < 1:
            time.sleep(1)  # Make the batch will fail correctly.
            self.workers[0].__ray_kill__()
        success = check_for_failure(worker_stats)
        return success, worker_stats

    with patch.object(TorchTrainer, "_train_epoch", step_with_fail):
        trainer1 = TorchTrainer(
            model_creator=model_creator,
            data_creator=single_loader,
            optimizer_creator=optimizer_creator,
            config=dict(batch_size=100000),
            loss_creator=lambda config: nn.MSELoss(),
            num_workers=2)

        @ray.remote
        def try_test():
            import time
            time.sleep(100)

        try_test.remote()
        trainer1.train(max_retries=1)
        assert len(trainer1.workers) == 1


def test_fail_twice(ray_start_2_cpus):  # noqa: F811
    if not dist.is_available():
        return

    def single_loader(config):
        dataset = LinearDataset(2, 5, size=1000000)
        return torch.utils.data.DataLoader(
            dataset, batch_size=config.get("batch_size", 32))

    def step_with_fail(self, *args, **kwargs):
        worker_stats = [
            w.train_epoch.remote(*args, **kwargs) for w in self.workers
        ]
        if self._num_failures < 2:
            time.sleep(1)
            self.workers[0].__ray_kill__()
        success = check_for_failure(worker_stats)
        return success, worker_stats

    with patch.object(TorchTrainer, "_train_epoch", step_with_fail):
        trainer1 = TorchTrainer(
            model_creator=model_creator,
            data_creator=single_loader,
            optimizer_creator=optimizer_creator,
            config=dict(batch_size=100000),
            loss_creator=lambda config: nn.MSELoss(),
            num_workers=2)

        trainer1.train(max_retries=2)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", "-x", __file__]))
