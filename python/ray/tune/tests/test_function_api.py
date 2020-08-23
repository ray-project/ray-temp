import json
import os
import shutil
import tempfile
import unittest

import ray
from ray.rllib import _register_all

from ray import tune
from ray.tune.logger import NoopLogger
from ray.tune.trainable import TrainableUtil
from ray.tune.function_runner import wrap_function, FuncCheckpointUtil
from ray.tune.result import TRAINING_ITERATION


def creator_generator(logdir):
    def logger_creator(config):
        return NoopLogger(config, logdir)

    return logger_creator


class FuncCheckpointUtilTest(unittest.TestCase):
    def setUp(self):
        self.logdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.logdir)

    def testEmptyCheckpoint(self):
        checkpoint_dir = FuncCheckpointUtil.mk_null_checkpoint_dir(self.logdir)
        assert FuncCheckpointUtil.is_null_checkpoint(checkpoint_dir)

    def testTempCheckpointDir(self):
        checkpoint_dir = FuncCheckpointUtil.mk_temp_checkpoint_dir(self.logdir)
        assert FuncCheckpointUtil.is_temp_checkpoint_dir(checkpoint_dir)

    def testConvertTempToPermanent(self):
        checkpoint_dir = FuncCheckpointUtil.mk_temp_checkpoint_dir(self.logdir)
        new_checkpoint_dir = FuncCheckpointUtil.create_perm_checkpoint(
            checkpoint_dir, self.logdir, step=4)
        assert new_checkpoint_dir == TrainableUtil.find_checkpoint_dir(
            new_checkpoint_dir)
        assert os.path.exists(new_checkpoint_dir)
        assert not FuncCheckpointUtil.is_temp_checkpoint_dir(
            new_checkpoint_dir)

        tmp_checkpoint_dir = FuncCheckpointUtil.mk_temp_checkpoint_dir(
            self.logdir)
        assert tmp_checkpoint_dir != new_checkpoint_dir


class FunctionCheckpointingTest(unittest.TestCase):
    def setUp(self):
        self.logdir = tempfile.mkdtemp()
        self.logger_creator = creator_generator(self.logdir)

    def tearDown(self):
        shutil.rmtree(self.logdir)

    def testCheckpointReuse(self):
        """Test that repeated save/restore never reuses same checkpoint dir."""

        def train(config, checkpoint_dir=None):
            if checkpoint_dir:
                count = sum("checkpoint-" in path
                            for path in os.listdir(checkpoint_dir))
                assert count == 1, os.listdir(checkpoint_dir)

            for step in range(20):
                with tune.checkpoint_dir(step=step) as checkpoint_dir:
                    path = os.path.join(checkpoint_dir,
                                        "checkpoint-{}".format(step))
                    open(path, "a").close()
                tune.report(test=step)

        wrapped = wrap_function(train)
        checkpoint = None
        for i in range(5):
            new_trainable = wrapped(logger_creator=self.logger_creator)
            if checkpoint:
                new_trainable.restore(checkpoint)
            for i in range(2):
                result = new_trainable.train()
            checkpoint = new_trainable.save()
            new_trainable.stop()
        assert result[TRAINING_ITERATION] == 10

    def testCheckpointReuseObject(self):
        """Test that repeated save/restore never reuses same checkpoint dir."""

        def train(config, checkpoint_dir=None):
            if checkpoint_dir:
                count = sum("checkpoint-" in path
                            for path in os.listdir(checkpoint_dir))
                assert count == 1, os.listdir(checkpoint_dir)

            for step in range(20):
                with tune.checkpoint_dir(step=step) as checkpoint_dir:
                    path = os.path.join(checkpoint_dir,
                                        "checkpoint-{}".format(step))
                    open(path, "a").close()
                tune.report(test=step)

        wrapped = wrap_function(train)
        checkpoint = None
        for i in range(5):
            new_trainable = wrapped(logger_creator=self.logger_creator)
            if checkpoint:
                new_trainable.restore_from_object(checkpoint)
            for i in range(2):
                result = new_trainable.train()
            checkpoint = new_trainable.save_to_object()
            new_trainable.stop()
        self.assertTrue(result[TRAINING_ITERATION] == 10)

    def testCheckpointReuseObjectWithoutTraining(self):
        """Test that repeated save/restore never reuses same checkpoint dir."""

        def train(config, checkpoint_dir=None):
            if checkpoint_dir:
                count = sum("checkpoint-" in path
                            for path in os.listdir(checkpoint_dir))
                assert count == 1, os.listdir(checkpoint_dir)

            for step in range(20):
                with tune.checkpoint_dir(step=step) as checkpoint_dir:
                    path = os.path.join(checkpoint_dir,
                                        "checkpoint-{}".format(step))
                    open(path, "a").close()
                tune.report(test=step)

        wrapped = wrap_function(train)
        new_trainable = wrapped(logger_creator=self.logger_creator)
        for i in range(2):
            result = new_trainable.train()
        checkpoint = new_trainable.save_to_object()
        new_trainable.stop()

        new_trainable2 = wrapped(logger_creator=self.logger_creator)
        new_trainable2.restore_from_object(checkpoint)
        new_trainable2.stop()

        new_trainable2 = wrapped(logger_creator=self.logger_creator)
        new_trainable2.restore_from_object(checkpoint)
        result = new_trainable2.train()
        new_trainable2.stop()
        self.assertTrue(result[TRAINING_ITERATION] == 3)

    def testReuseNullCheckpoint(self):
        def train(config, checkpoint_dir=None):
            assert not checkpoint_dir
            for step in range(10):
                tune.report(test=step)

        # Create checkpoint
        wrapped = wrap_function(train)
        checkpoint = None
        new_trainable = wrapped(logger_creator=self.logger_creator)
        new_trainable.train()
        checkpoint = new_trainable.save()
        new_trainable.stop()

        # Use the checkpoint a couple of times
        for i in range(3):
            new_trainable = wrapped(logger_creator=self.logger_creator)
            new_trainable.restore(checkpoint)
            new_trainable.stop()

        # Make sure the result is still good
        new_trainable = wrapped(logger_creator=self.logger_creator)
        new_trainable.restore(checkpoint)
        result = new_trainable.train()
        checkpoint = new_trainable.save()
        new_trainable.stop()
        self.assertTrue(result[TRAINING_ITERATION] == 1)

    def testMultipleNullCheckpoints(self):
        def train(config, checkpoint_dir=None):
            assert not checkpoint_dir
            for step in range(10):
                tune.report(test=step)

        wrapped = wrap_function(train)
        checkpoint = None
        for i in range(5):
            new_trainable = wrapped(logger_creator=self.logger_creator)
            if checkpoint:
                new_trainable.restore(checkpoint)
            result = new_trainable.train()
            checkpoint = new_trainable.save()
            new_trainable.stop()
        self.assertTrue(result[TRAINING_ITERATION] == 1)

    def testMultipleNullMemoryCheckpoints(self):
        def train(config, checkpoint_dir=None):
            assert not checkpoint_dir
            for step in range(10):
                tune.report(test=step)

        wrapped = wrap_function(train)
        checkpoint = None
        for i in range(5):
            new_trainable = wrapped(logger_creator=self.logger_creator)
            if checkpoint:
                new_trainable.restore_from_object(checkpoint)
            result = new_trainable.train()
            checkpoint = new_trainable.save_to_object()
            new_trainable.stop()
        assert result[TRAINING_ITERATION] == 1

    def testFunctionNoCheckpointing(self):
        def train(config, checkpoint_dir=None):
            if checkpoint_dir:
                assert os.path.exists(checkpoint_dir)
            for step in range(10):
                tune.report(test=step)

        wrapped = wrap_function(train)

        new_trainable = wrapped(logger_creator=self.logger_creator)
        result = new_trainable.train()
        checkpoint = new_trainable.save()
        new_trainable.stop()

        new_trainable2 = wrapped(logger_creator=self.logger_creator)
        new_trainable2.restore(checkpoint)
        result = new_trainable2.train()
        self.assertEquals(result[TRAINING_ITERATION], 1)
        checkpoint = new_trainable2.save()
        new_trainable2.stop()

    def testFunctionRecurringSave(self):
        """This tests that save and restore are commutative."""

        def train(config, checkpoint_dir=None):
            if checkpoint_dir:
                assert os.path.exists(checkpoint_dir)
            for step in range(10):
                if step % 3 == 0:
                    with tune.checkpoint_dir(step=step) as checkpoint_dir:
                        path = os.path.join(checkpoint_dir, "checkpoint")
                        with open(path, "w") as f:
                            f.write(json.dumps({"step": step}))
                tune.report(test=step)

        wrapped = wrap_function(train)

        new_trainable = wrapped(logger_creator=self.logger_creator)
        new_trainable.train()
        checkpoint_obj = new_trainable.save_to_object()
        new_trainable.restore_from_object(checkpoint_obj)
        checkpoint = new_trainable.save()

        new_trainable.stop()

        new_trainable2 = wrapped(logger_creator=self.logger_creator)
        new_trainable2.restore(checkpoint)
        new_trainable2.train()
        new_trainable2.stop()

    def testFunctionImmediateSave(self):
        """This tests that save and restore are commutative."""

        def train(config, checkpoint_dir=None):
            if checkpoint_dir:
                assert os.path.exists(checkpoint_dir)
            for step in range(10):
                with tune.checkpoint_dir(step=step) as checkpoint_dir:
                    print(checkpoint_dir)
                    path = os.path.join(checkpoint_dir,
                                        "checkpoint-{}".format(step))
                    open(path, "w").close()
                tune.report(test=step)

        wrapped = wrap_function(train)
        new_trainable = wrapped(logger_creator=self.logger_creator)
        new_trainable.train()
        new_trainable.train()
        checkpoint_obj = new_trainable.save_to_object()
        new_trainable.stop()

        new_trainable2 = wrapped(logger_creator=self.logger_creator)
        new_trainable2.restore_from_object(checkpoint_obj)
        checkpoint_obj = new_trainable2.save_to_object()
        new_trainable2.train()
        result = new_trainable2.train()
        assert sum("tmp" in path for path in os.listdir(self.logdir)) == 1
        new_trainable2.stop()
        assert sum("tmp" in path for path in os.listdir(self.logdir)) == 0
        assert result[TRAINING_ITERATION] == 4


class FunctionApiTest(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=4, num_gpus=0, object_store_memory=150 * 1024 * 1024)

    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects

    def testCheckpointError(self):
        def train(config, checkpoint_dir=False):
            pass

        with self.assertRaises(ValueError):
            tune.run(train, checkpoint_freq=1)
        with self.assertRaises(ValueError):
            tune.run(train, checkpoint_at_end=True)

    def testCheckpointFunctionAtEnd(self):
        def train(config, checkpoint_dir=False):
            for i in range(10):
                tune.report(test=i)
            with tune.checkpoint_dir(step=10) as checkpoint_dir:
                checkpoint_path = os.path.join(checkpoint_dir, "ckpt.log")
                with open(checkpoint_path, "w") as f:
                    f.write("hello")

        [trial] = tune.run(train).trials
        assert os.path.exists(os.path.join(trial.checkpoint.value, "ckpt.log"))

    def testCheckpointFunctionAtEndContext(self):
        def train(config, checkpoint_dir=False):
            for i in range(10):
                tune.report(test=i)
            with tune.checkpoint_dir(step=10) as checkpoint_dir:
                checkpoint_path = os.path.join(checkpoint_dir, "ckpt.log")
                with open(checkpoint_path, "w") as f:
                    f.write("hello")

        [trial] = tune.run(train).trials
        assert os.path.exists(os.path.join(trial.checkpoint.value, "ckpt.log"))

    def testVariousCheckpointFunctionAtEnd(self):
        def train(config, checkpoint_dir=False):
            for i in range(10):
                with tune.checkpoint_dir(step=i) as checkpoint_dir:
                    checkpoint_path = os.path.join(checkpoint_dir, "ckpt.log")
                    with open(checkpoint_path, "w") as f:
                        f.write("hello")
                tune.report(test=i)
            with tune.checkpoint_dir(step=i) as checkpoint_dir:
                checkpoint_path = os.path.join(checkpoint_dir, "ckpt.log2")
                with open(checkpoint_path, "w") as f:
                    f.write("goodbye")

        [trial] = tune.run(train, keep_checkpoints_num=3).trials
        assert os.path.exists(
            os.path.join(trial.checkpoint.value, "ckpt.log2"))

    def testReuseCheckpoint(self):
        def train(config, checkpoint_dir=None):
            itr = 0
            if checkpoint_dir:
                with open(os.path.join(checkpoint_dir, "ckpt.log"), "r") as f:
                    itr = int(f.read()) + 1

            for i in range(itr, config["max_iter"]):
                with tune.checkpoint_dir(step=i) as checkpoint_dir:
                    checkpoint_path = os.path.join(checkpoint_dir, "ckpt.log")
                    with open(checkpoint_path, "w") as f:
                        f.write(str(i))
                tune.report(test=i, training_iteration=i)

        [trial] = tune.run(
            train,
            config={
                "max_iter": 5
            },
        ).trials
        last_ckpt = trial.checkpoint.value
        assert os.path.exists(os.path.join(trial.checkpoint.value, "ckpt.log"))
        analysis = tune.run(train, config={"max_iter": 10}, restore=last_ckpt)
        trial_dfs = list(analysis.trial_dataframes.values())
        assert len(trial_dfs[0]["training_iteration"]) == 5

    def testRetry(self):
        def train(config, checkpoint_dir=None):
            restored = bool(checkpoint_dir)
            itr = 0
            if checkpoint_dir:
                with open(os.path.join(checkpoint_dir, "ckpt.log"), "r") as f:
                    itr = int(f.read()) + 1

            for i in range(itr, 10):
                if i == 5 and not restored:
                    raise Exception("try to fail me")
                with tune.checkpoint_dir(step=i) as checkpoint_dir:
                    checkpoint_path = os.path.join(checkpoint_dir, "ckpt.log")
                    with open(checkpoint_path, "w") as f:
                        f.write(str(i))
                tune.report(test=i, training_iteration=i)

        analysis = tune.run(train, max_failures=3)
        last_ckpt = analysis.trials[0].checkpoint.value
        assert os.path.exists(os.path.join(last_ckpt, "ckpt.log"))
        trial_dfs = list(analysis.trial_dataframes.values())
        assert len(trial_dfs[0]["training_iteration"]) == 10

    def testBlankCheckpoint(self):
        def train(config, checkpoint_dir=None):
            restored = bool(checkpoint_dir)
            itr = 0
            if checkpoint_dir:
                with open(os.path.join(checkpoint_dir, "ckpt.log"), "r") as f:
                    itr = int(f.read()) + 1

            for i in range(itr, 10):
                if i == 5 and not restored:
                    raise Exception("try to fail me")
                with tune.checkpoint_dir(step=itr) as checkpoint_dir:
                    checkpoint_path = os.path.join(checkpoint_dir, "ckpt.log")
                    with open(checkpoint_path, "w") as f:
                        f.write(str(i))
                tune.report(test=i, training_iteration=i)

        analysis = tune.run(train, max_failures=3)
        trial_dfs = list(analysis.trial_dataframes.values())
        assert len(trial_dfs[0]["training_iteration"]) == 10
