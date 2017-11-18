from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import os

import ray
from ray.rllib import _register_all
from ray.tune.trial import Trial, Resources
from ray.tune.trial_runner import TrialRunner
from ray.tune.variant_generator import generate_trials, grid_search, \
    RecursiveDependencyError


class VariantGeneratorTest(unittest.TestCase):
    def testParseToTrials(self):
        trials = generate_trials({
            "run": "PPO",
            "repeat": 2,
            "config": {
                "env": "Pong-v0",
                "foo": "bar"
            },
        }, "tune-pong")
        trials = list(trials)
        self.assertEqual(len(trials), 2)
        self.assertEqual(str(trials[0]), "PPO_Pong-v0_0")
        self.assertEqual(trials[0].config, {"foo": "bar", "env": "Pong-v0"})
        self.assertEqual(trials[0].trainable_name, "PPO")
        self.assertEqual(trials[0].experiment_tag, "0")
        self.assertEqual(trials[0].local_dir, "/tmp/ray/tune-pong")
        self.assertEqual(trials[1].experiment_tag, "1")

    def testEval(self):
        trials = generate_trials({
            "run": "PPO",
            "config": {
                "foo": {
                    "eval": "2 + 2"
                },
            },
        })
        trials = list(trials)
        self.assertEqual(len(trials), 1)
        self.assertEqual(trials[0].config, {"foo": 4})
        self.assertEqual(trials[0].experiment_tag, "0_foo=4")
        self.assertEqual(trials[0].local_dir, "/tmp/ray/")

    def testGridSearch(self):
        trials = generate_trials({
            "run": "PPO",
            "config": {
                "bar": {
                    "grid_search": [True, False]
                },
                "foo": {
                    "grid_search": [1, 2, 3]
                },
            },
        })
        trials = list(trials)
        self.assertEqual(len(trials), 6)
        self.assertEqual(trials[0].config, {"bar": True, "foo": 1})
        self.assertEqual(trials[0].experiment_tag, "0_bar=True,foo=1")
        self.assertEqual(trials[1].config, {"bar": False, "foo": 1})
        self.assertEqual(trials[1].experiment_tag, "1_bar=False,foo=1")
        self.assertEqual(trials[2].config, {"bar": True, "foo": 2})
        self.assertEqual(trials[3].config, {"bar": False, "foo": 2})
        self.assertEqual(trials[4].config, {"bar": True, "foo": 3})
        self.assertEqual(trials[5].config, {"bar": False, "foo": 3})

    def testGridSearchAndEval(self):
        trials = generate_trials({
            "run": "PPO",
            "config": {
                "qux": lambda spec: 2 + 2,
                "bar": grid_search([True, False]),
                "foo": grid_search([1, 2, 3]),
            },
        })
        trials = list(trials)
        self.assertEqual(len(trials), 6)
        self.assertEqual(trials[0].config, {"bar": True, "foo": 1, "qux": 4})
        self.assertEqual(trials[0].experiment_tag, "0_bar=True,foo=1,qux=4")

    def testConditionResolution(self):
        trials = generate_trials({
            "run": "PPO",
            "config": {
                "x": 1,
                "y": lambda spec: spec.config.x + 1,
                "z": lambda spec: spec.config.y + 1,
            },
        })
        trials = list(trials)
        self.assertEqual(len(trials), 1)
        self.assertEqual(trials[0].config, {"x": 1, "y": 2, "z": 3})

    def testDependentLambda(self):
        trials = generate_trials({
            "run": "PPO",
            "config": {
                "x": grid_search([1, 2]),
                "y": lambda spec: spec.config.x * 100,
            },
        })
        trials = list(trials)
        self.assertEqual(len(trials), 2)
        self.assertEqual(trials[0].config, {"x": 1, "y": 100})
        self.assertEqual(trials[1].config, {"x": 2, "y": 200})

    def testDependentGridSearch(self):
        trials = generate_trials({
            "run": "PPO",
            "config": {
                "x": grid_search([
                    lambda spec: spec.config.y * 100,
                    lambda spec: spec.config.y * 200
                ]),
                "y": lambda spec: 1,
            },
        })
        trials = list(trials)
        self.assertEqual(len(trials), 2)
        self.assertEqual(trials[0].config, {"x": 100, "y": 1})
        self.assertEqual(trials[1].config, {"x": 200, "y": 1})

    def testRecursiveDep(self):
        try:
            list(generate_trials({
                "run": "PPO",
                "config": {
                    "foo": lambda spec: spec.config.foo,
                },
            }))
        except RecursiveDependencyError as e:
            assert "`foo` recursively depends on" in str(e), e
        else:
            assert False


class TrialRunnerTest(unittest.TestCase):
    def tearDown(self):
        ray.worker.cleanup()
        _register_all()  # re-register the evicted objects

    def testTrialStatus(self):
        ray.init()
        trial = Trial("__fake")
        self.assertEqual(trial.status, Trial.PENDING)
        trial.start()
        self.assertEqual(trial.status, Trial.RUNNING)
        trial.stop()
        self.assertEqual(trial.status, Trial.TERMINATED)
        trial.stop(error=True)
        self.assertEqual(trial.status, Trial.ERROR)

    def testTrialErrorOnStart(self):
        ray.init()
        trial = Trial("asdf")
        try:
            trial.start()
        except Exception as e:
            self.assertIn("Unknown trainable", str(e))

    def testResourceScheduler(self):
        ray.init(num_cpus=4, num_gpus=1)
        runner = TrialRunner()
        kwargs = {
            "stopping_criterion": {"training_iteration": 1},
            "resources": Resources(cpu=1, gpu=1),
        }
        trials = [
            Trial("__fake", **kwargs),
            Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.RUNNING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.TERMINATED)

    def testMultiStepRun(self):
        ray.init(num_cpus=4, num_gpus=2)
        runner = TrialRunner()
        kwargs = {
            "stopping_criterion": {"training_iteration": 5},
            "resources": Resources(cpu=1, gpu=1),
        }
        trials = [
            Trial("__fake", **kwargs),
            Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.RUNNING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.RUNNING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.RUNNING)

    def testErrorHandling(self):
        ray.init(num_cpus=4, num_gpus=2)
        runner = TrialRunner()
        kwargs = {
            "stopping_criterion": {"training_iteration": 1},
            "resources": Resources(cpu=1, gpu=1),
        }
        trials = [
            Trial("asdf", **kwargs),
            Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        runner.step()
        self.assertEqual(trials[0].status, Trial.ERROR)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.ERROR)
        self.assertEqual(trials[1].status, Trial.RUNNING)

    def testCheckpointing(self):
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner()
        kwargs = {
            "stopping_criterion": {"training_iteration": 1},
            "resources": Resources(cpu=1, gpu=1),
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(ray.get(trials[0].runner.set_info.remote(1)), 1)

        path = trials[0].checkpoint()
        kwargs["restore_path"] = path

        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.RUNNING)
        self.assertEqual(ray.get(trials[1].runner.get_info.remote()), 1)
        self.addCleanup(os.remove, path)

    def testPauseThenResume(self):
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner()
        kwargs = {
            "stopping_criterion": {"training_iteration": 2},
            "resources": Resources(cpu=1, gpu=1),
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(ray.get(trials[0].runner.get_info.remote()), None)

        self.assertEqual(ray.get(trials[0].runner.set_info.remote(1)), 1)

        trials[0].pause()
        self.assertEqual(trials[0].status, Trial.PAUSED)

        trials[0].resume()
        self.assertEqual(trials[0].status, Trial.RUNNING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(ray.get(trials[0].runner.get_info.remote()), 1)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)


if __name__ == "__main__":
    unittest.main(verbosity=2)
