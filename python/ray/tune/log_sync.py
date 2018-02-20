from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import distutils.spawn
import os
import shlex
import subprocess
import time
import traceback

import ray
from ray.tune.cluster_info import get_ssh_key, get_ssh_user
from ray.tune.error import TuneError
from ray.tune.result import DEFAULT_RESULTS_DIR


# Map from (logdir, remote_dir) -> syncer
_syncers = {}


def get_syncer(local_dir, remote_dir=None):
    if remote_dir:
        if not remote_dir.startswith("s3://"):
            raise TuneError("Upload uri must start with s3://")

        if not distutils.spawn.find_executable("aws"):
            raise TuneError("Upload uri requires awscli tool to be installed")

        if local_dir.startswith(DEFAULT_RESULTS_DIR + "/"):
            rel_path = os.path.relpath(local_dir, DEFAULT_RESULTS_DIR)
            remote_dir = os.path.join(remote_dir, rel_path)

    key = (local_dir, remote_dir)
    if key not in _syncers:
        _syncers[key] = _LogSyncer(local_dir, remote_dir)

    return _syncers[key]


def wait_for_log_sync():
    for syncer in _syncers.values():
        syncer.wait()


class _LogSyncer(object):
    """Log syncer for tune.

    This syncs files from workers to the local node, and optionally also from
    the local node to a remote directory (e.g. S3)."""

    def __init__(self, local_dir, remote_dir=None):
        self.local_dir = local_dir
        self.remote_dir = remote_dir
        self.last_sync_time = 0
        self.sync_process = None
        self.local_ip = ray.services.get_node_ip_address()
        self.worker_ip = None
        self.worker_ip_fut = None
        print("Created LogSyncer for {} -> {}".format(local_dir, remote_dir))

    def set_worker_ip_fut(self, worker_ip_fut):
        self.worker_ip_fut = worker_ip_fut

    def refresh_worker_ip(self):
        if self.worker_ip_fut:
            try:
                self.worker_ip = ray.get(self.worker_ip_fut)
            except Exception:
                error_msg = traceback.format_exc()
                print("Error getting worker ip:", error_msg)
                self.worker_ip = None
                self.worker_ip_fut = None

    def sync_if_needed(self):
        if time.time() - self.last_sync_time > 300:
            self.refresh_worker_ip()
            self.sync_now()

    def sync_now(self, force=False):
        self.last_sync_time = time.time()
        if not self.worker_ip:
            print(
                "Worker ip unknown, skipping log sync for {}".format(
                    self.local_dir))
            return

        if self.worker_ip == self.local_ip:
            worker_to_local_sync_cmd = None  # don't need to rsync
        else:
            ssh_key = get_ssh_key()
            ssh_user = get_ssh_user()
            if not distutils.spawn.find_executable("rsync"):
                raise TuneError(
                    "Log sync requires the rsync tool to be installed")
            worker_to_local_sync_cmd = (
                """rsync -avz -e "ssh -i '{}'" '{}@{}:{}/' '{}/'""".format(
                    ssh_key, ssh_user, self.worker_ip,
                    shlex.quote(self.local_dir), shlex.quote(self.local_dir)))

        if self.remote_dir:
            local_to_remote_sync_cmd = (
                "aws s3 sync '{}' '{}'".format(
                    shlex.quote(self.local_dir), shlex.quote(self.remote_dir)))
        else:
            local_to_remote_sync_cmd = None

        if self.sync_process:
            self.sync_process.poll()
            if self.sync_process.returncode is None:
                if force:
                    self.sync_process.kill()
                else:
                    print("Warning: last sync is still in progress, skipping")
                    return

        if worker_to_local_sync_cmd or local_to_remote_sync_cmd:
            final_cmd = ""
            if worker_to_local_sync_cmd:
                final_cmd += worker_to_local_sync_cmd
            if local_to_remote_sync_cmd:
                if final_cmd:
                    final_cmd += " && "
                final_cmd += local_to_remote_sync_cmd
            print("Running log sync: {}".format(final_cmd))
            self.sync_process = subprocess.Popen(final_cmd, shell=True)

    def wait(self):
        if self.sync_process:
            self.sync_process.wait()
