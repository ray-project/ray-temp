from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pytest

from ray.test.cluster_utils import Cluster
import ray
from ray import tune
from ray.tune.error import TuneError
from ray.tune.experiment import Experiment
from ray.tune.trial import Trial, Resources
from ray.tune.trial_runner import TrialRunner
from ray.tune.suggest import grid_search, BasicVariantGenerator


@pytest.fixture
def start_connected_cluster():
    # Start the Ray processes.
    cluster = Cluster(
        initialize_head=True, connect=True,
        head_node_args={"resources": dict(CPU=1)})
    yield cluster
    # The code after the yield will run as teardown code.
    ray.shutdown()
    cluster.shutdown()


class _Train(tune.Trainable):
    def _setup(self, config):
        self.state = {"hi": 1}

    def _train(self):
        self.state["hi"] += 1
        return {}

    def _save(self, path):
        return self.state

    def _restore(self, state):
        self.state = state


def test_counting_resources(start_connected_cluster):
    """Removing a node should cause a Trial to be requeued."""
    cluster = start_connected_cluster
    node = cluster.add_node(resources=dict(CPU=1))

    runner = TrialRunner(BasicVariantGenerator())
    kwargs = {
        "stopping_criterion": {
            "training_iteration": 3
        }
    }

    tune.register_trainable("test", _Train)
    trials = [Trial("test", **kwargs), Trial("test", **kwargs)]
    for t in trials:
        runner.add_trial(t)

    runner.step()  # run 1
    cluster.remove_node(node)
    runner.step()  # run 2
    node = cluster.add_node(resources=dict(CPU=1))

    assert all(t.status == Trial.RUNNING for t in trials)

    runner.step()  # 1 result
    print(runner.debug_string())

    cluster.remove_node(node)

    runner.step()  # recover
    for i in range(5):
        runner.step()
    assert all(t.status == Trial.TERMINATED for t in trials)

    with pytest.raises(TuneError):
        runner.step()


def test_remove_node_before_result(start_connected_cluster):
    """Removing a node should cause a Trial to be requeued."""
    cluster = start_connected_cluster
    node = cluster.add_node(resources=dict(CPU=1))

    runner = TrialRunner(BasicVariantGenerator())
    kwargs = {
        "stopping_criterion": {
            "training_iteration": 3
        }
    }

    tune.register_trainable("test", _Train)
    trials = [Trial("test", **kwargs), Trial("test", **kwargs)]
    for t in trials:
        runner.add_trial(t)

    runner.step()  # run 1
    runner.step()  # run 2
    assert all(t.status == Trial.RUNNING for t in trials)

    runner.step()  # 1 result
    print(runner.debug_string())

    cluster.remove_node(node)

    runner.step()  # recover
    for i in range(5):
        runner.step()
    assert all(t.status == Trial.TERMINATED for t in trials)

    with pytest.raises(TuneError):
        runner.step()


def test_trial_migration(start_connected_cluster):
    """Removing a node should cause a Trial to be requeued."""
    cluster = start_connected_cluster
    node = cluster.add_node(resources=dict(CPU=1))

    runner = TrialRunner(BasicVariantGenerator())
    kwargs = {
        "stopping_criterion": {
            "training_iteration": 3
        }
    }

    tune.register_trainable("test", _Train)
    trials = [Trial("test", **kwargs), Trial("test", **kwargs)]
    for t in trials:
        runner.add_trial(t)

    runner.step()  # run 1
    runner.step()  # run 2
    assert all(t.status == Trial.RUNNING for t in trials)

    runner.step()  # 1 result
    print(runner.debug_string())

    cluster.remove_node(node)
    node2 = cluster.add_node(resources=dict(CPU=1))

    runner.step()  # recover
    for i in range(5):
        runner.step()
        print(runner.debug_string())
    assert all(t.status == Trial.TERMINATED for t in trials)

    with pytest.raises(TuneError):
        runner.step()
