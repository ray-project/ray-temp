import glob
import os
import shutil
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import ray
from ray.rllib import _register_all

from ray import tune
from ray.tune import TuneError
from ray.tune.syncer import CommandBasedClient


class TestSyncFunctionality(unittest.TestCase):
    def setUp(self):
        ray.init()

    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects

    @patch("ray.tune.sync_client.S3_PREFIX", "test")
    def testNoUploadDir(self):
        """No Upload Dir is given."""
        with self.assertRaises(AssertionError):
            [trial] = tune.run(
                "__fake",
                name="foo",
                max_failures=0,
                stop={
                    "training_iteration": 1
                },
                sync_config=tune.SyncConfig(
                    **{"sync_to_cloud": "echo {source} {target}"})).trials

    @patch("ray.tune.sync_client.S3_PREFIX", "test")
    def testCloudProperString(self):
        with self.assertRaises(ValueError):
            [trial] = tune.run(
                "__fake",
                name="foo",
                max_failures=0,
                stop={
                    "training_iteration": 1
                },
                sync_config=tune.SyncConfig(**{
                    "upload_dir": "test",
                    "sync_to_cloud": "ls {target}"
                })).trials

        with self.assertRaises(ValueError):
            [trial] = tune.run(
                "__fake",
                name="foo",
                max_failures=0,
                stop={
                    "training_iteration": 1
                },
                sync_config=tune.SyncConfig(**{
                    "upload_dir": "test",
                    "sync_to_cloud": "ls {source}"
                })).trials

        tmpdir = tempfile.mkdtemp()
        logfile = os.path.join(tmpdir, "test.log")

        [trial] = tune.run(
            "__fake",
            name="foo",
            max_failures=0,
            stop={
                "training_iteration": 1
            },
            sync_config=tune.SyncConfig(
                **{
                    "upload_dir": "test",
                    "sync_to_cloud": "echo {source} {target} > " + logfile
                })).trials
        with open(logfile) as f:
            lines = f.read()
            self.assertTrue("test" in lines)
        shutil.rmtree(tmpdir)

    def testClusterProperString(self):
        """Tests that invalid commands throw.."""
        with self.assertRaises(TuneError):
            # This raises TuneError because logger is init in safe zone.
            sync_config = tune.SyncConfig(sync_to_driver="ls {target}")
            [trial] = tune.run(
                "__fake",
                name="foo",
                max_failures=0,
                stop={
                    "training_iteration": 1
                },
                sync_config=sync_config,
            ).trials

        with self.assertRaises(TuneError):
            # This raises TuneError because logger is init in safe zone.
            sync_config = tune.SyncConfig(sync_to_driver="ls {source}")
            [trial] = tune.run(
                "__fake",
                name="foo",
                max_failures=0,
                sync_config=sync_config,
                stop={
                    "training_iteration": 1
                }).trials

        with patch.object(CommandBasedClient, "_execute") as mock_fn:
            with patch("ray.services.get_node_ip_address") as mock_sync:
                sync_config = tune.SyncConfig(
                    sync_to_driver="echo {source} {target}")
                mock_sync.return_value = "0.0.0.0"
                [trial] = tune.run(
                    "__fake",
                    name="foo",
                    max_failures=0,
                    sync_config=sync_config,
                    stop={
                        "training_iteration": 1
                    }).trials
                self.assertGreater(mock_fn.call_count, 0)

    def testCloudFunctions(self):
        tmpdir = tempfile.mkdtemp()
        tmpdir2 = tempfile.mkdtemp()
        os.mkdir(os.path.join(tmpdir2, "foo"))

        def sync_func(local, remote):
            for filename in glob.glob(os.path.join(local, "*.json")):
                shutil.copy(filename, remote)

        sync_config = tune.SyncConfig(
            upload_dir=tmpdir2, sync_to_cloud=sync_func)
        [trial] = tune.run(
            "__fake",
            name="foo",
            max_failures=0,
            local_dir=tmpdir,
            stop={
                "training_iteration": 1
            },
            sync_config=sync_config).trials
        test_file_path = glob.glob(os.path.join(tmpdir2, "foo", "*.json"))
        self.assertTrue(test_file_path)
        shutil.rmtree(tmpdir)
        shutil.rmtree(tmpdir2)

    @patch("ray.tune.sync_client.S3_PREFIX", "test")
    def testCloudSyncPeriod(self):
        """Tests that changing CLOUD_SYNC_PERIOD affects syncing frequency."""
        tmpdir = tempfile.mkdtemp()

        def trainable(config):
            for i in range(10):
                time.sleep(1)
                tune.report(score=i)

        mock = unittest.mock.Mock()

        def counter(local, remote):
            mock()

        sync_config = tune.SyncConfig(upload_dir="test", sync_to_cloud=counter)
        tune.syncer.CLOUD_SYNC_PERIOD = 1
        os.environ["TUNE_GLOBAL_CHECKPOINT_S"] = "0"
        self.addCleanup(
            lambda: os.environ.pop("TUNE_GLOBAL_CHECKPOINT_S", None))
        [trial] = tune.run(
            trainable,
            name="foo",
            max_failures=0,
            local_dir=tmpdir,
            stop={
                "training_iteration": 10
            },
            sync_config=sync_config,
        ).trials

        self.assertEqual(mock.call_count, 12)
        shutil.rmtree(tmpdir)

    def testClusterSyncFunction(self):
        def sync_func_driver(source, target):
            assert ":" in source, "Source {} not a remote path.".format(source)
            assert ":" not in target, "Target is supposed to be local."
            with open(os.path.join(target, "test.log2"), "w") as f:
                print("writing to", f.name)
                f.write(source)

        sync_config = tune.SyncConfig(sync_to_driver=sync_func_driver)

        [trial] = tune.run(
            "__fake",
            name="foo",
            max_failures=0,
            stop={
                "training_iteration": 1
            },
            sync_config=sync_config).trials
        test_file_path = os.path.join(trial.logdir, "test.log2")
        self.assertFalse(os.path.exists(test_file_path))

        with patch("ray.services.get_node_ip_address") as mock_sync:
            mock_sync.return_value = "0.0.0.0"
            sync_config = tune.SyncConfig(sync_to_driver=sync_func_driver)
            [trial] = tune.run(
                "__fake",
                name="foo",
                max_failures=0,
                stop={
                    "training_iteration": 1
                },
                sync_config=sync_config).trials
        test_file_path = os.path.join(trial.logdir, "test.log2")
        self.assertTrue(os.path.exists(test_file_path))
        os.remove(test_file_path)

    def testNoSync(self):
        """Sync should not run on a single node."""

        def sync_func(source, target):
            pass

        sync_config = tune.SyncConfig(sync_to_driver=sync_func)

        with patch.object(CommandBasedClient, "_execute") as mock_sync:
            [trial] = tune.run(
                "__fake",
                name="foo",
                max_failures=0,
                stop={
                    "training_iteration": 1
                },
                sync_config=sync_config).trials
            self.assertEqual(mock_sync.call_count, 0)


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
