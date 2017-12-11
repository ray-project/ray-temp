from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import subprocess
import sys
import tempfile
import time

from multiprocessing import Process

from ray.autoscaler.node_provider import get_node_provider
from ray.autoscaler.tags import TAG_RAY_WORKER_STATUS, TAG_RAY_APPLIED_CONFIG

# How long to wait for a node to start, in seconds
NODE_START_WAIT_S = 300


class NodeUpdater(Process):
    def __init__(
            self, node_id, provider, worker_group, file_mounts,
            init_cmds, config_hash):
        Process.__init__(self)
        self.provider = get_node_provider(provider, worker_group, None)
        self.node_id = node_id
        self.file_mounts = file_mounts
        self.init_cmds = init_cmds
        self.config_hash = config_hash
        self.logfile = tempfile.NamedTemporaryFile(
            prefix='node-updater-', delete=False)
        self.successful = False

    def run(self):
        print("NodeUpdater: Updating {} to {}, remote logs at {}".format(
            self.node_id, self.config_hash, self.logfile.name))
        try:
            self.do_update()
        except Exception as e:
            print(
                "NodeUpdater: Error updating {}, "
                "see {} for remote logs".format(e, self.logfile.name))
            self.provider.set_node_tags(
                self.node_id, {TAG_RAY_WORKER_STATUS: "UpdateFailed"})
            print(
                "----- BEGIN REMOTE LOGS -----\n" +
                open(self.logfile.name).read() +
                "\n----- END REMOTE LOGS -----")
            sys.exit(1)
        self.provider.set_node_tags(
            self.node_id, {
                TAG_RAY_WORKER_STATUS: "Up-to-date",
                TAG_RAY_APPLIED_CONFIG: self.config_hash
            })
        print("NodeUpdater: Applied config {} to node {}".format(
            self.config_hash, self.node_id))

    def do_update(self):
        external_ip = self.provider.external_ip(self.node_id)
        self.provider.set_node_tags(
            self.node_id, {TAG_RAY_WORKER_STATUS: "WaitingForSSH"})
        deadline = time.monotonic() + NODE_START_WAIT_S
        while time.monotonic() < deadline:
            try:
                subprocess.check_call([
                    "ssh", "-o", "ConnectTimeout=2s",
                    "-o", "StrictHostKeyChecking=no",
                    "-i", "~/.ssh/ekl-laptop-thinkpad.pem",
                    "ubuntu@{}".format(external_ip),
                    "uptime",
                ], stdout=self.logfile, stderr=self.logfile)
                time.sleep(5)
            except Exception:
                pass
            else:
                break
        self.provider.set_node_tags(
            self.node_id, {TAG_RAY_WORKER_STATUS: "SyncingFiles"})
        for remote_dir, local_dir in self.file_mounts.items():
            assert os.path.isdir(local_dir)
            subprocess.check_call([
                "rsync", "-e", "ssh -i ~/.ssh/ekl-laptop-thinkpad.pem "
                "-o ConnectTimeout=60s -o StrictHostKeyChecking=no",
                "--delete", "-avz", "{}/".format(local_dir),
                "ubuntu@{}:{}/".format(external_ip, remote_dir)
            ], stdout=self.logfile, stderr=self.logfile)
        self.provider.set_node_tags(
            self.node_id, {TAG_RAY_WORKER_STATUS: "RunningInitCmds"})
        for cmd in self.init_cmds:
            subprocess.check_call([
                "ssh", "-o", "ConnectTimeout=60s",
                "-o", "StrictHostKeyChecking=no",
                "-i", "~/.ssh/ekl-laptop-thinkpad.pem",
                "ubuntu@{}".format(external_ip),
                cmd,
            ], stdout=self.logfile, stderr=self.logfile)
