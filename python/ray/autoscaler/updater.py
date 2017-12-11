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
            self, node_id, provider_config, worker_group, file_mounts,
            init_cmds, files_hash, redirect_output=True):
        Process.__init__(self)
        self.provider = get_node_provider(provider_config, worker_group)
        self.node_id = node_id
        self.file_mounts = file_mounts
        self.init_cmds = init_cmds
        self.files_hash = files_hash
        if redirect_output:
            self.logfile = tempfile.NamedTemporaryFile(
                prefix='node-updater-', delete=False)
            self.output_name = self.logfile.name
            self.stdout = logfile
            self.stderr = logfile
        else:
            self.logfile = None
            self.output_name = '(console)'
            self.stdout = sys.stdout
            self.stderr = sys.stderr

    def run(self):
        print("NodeUpdater: Updating {} to {}, remote logs at {}".format(
            self.node_id, self.files_hash, self.output_name))
        try:
            self.do_update()
        except Exception as e:
            print(
                "NodeUpdater: Error updating {}, "
                "see {} for remote logs".format(e, self.output_name))
            self.provider.set_node_tags(
                self.node_id, {TAG_RAY_WORKER_STATUS: "UpdateFailed"})
            if self.logfile is not None:
                print(
                    "----- BEGIN REMOTE LOGS -----\n" +
                    open(self.logfile.name).read() +
                    "\n----- END REMOTE LOGS -----")
            sys.exit(1)
        self.provider.set_node_tags(
            self.node_id, {
                TAG_RAY_WORKER_STATUS: "Up-to-date",
                TAG_RAY_APPLIED_CONFIG: self.files_hash
            })
        print("NodeUpdater: Applied config {} to node {}".format(
            self.files_hash, self.node_id))

    def do_update(self):
        external_ip = self.provider.external_ip(self.node_id)
        self.provider.set_node_tags(
            self.node_id, {TAG_RAY_WORKER_STATUS: "WaitingForSSH"})
        deadline = time.monotonic() + NODE_START_WAIT_S
        while time.monotonic() < deadline and \
                not self.provider.is_terminated(self.node_id):
            try:
                if not self.provider.is_running(self.node_id):
                    raise Exception()
                self.ssh_cmd(external_ip, "true", connect_timeout=2)
            except Exception:
                time.sleep(5)
            else:
                break
        self.provider.set_node_tags(
            self.node_id, {TAG_RAY_WORKER_STATUS: "SyncingFiles"})
        for remote_path, local_path in self.file_mounts.items():
            assert os.path.exists(local_path)
            if os.path.isdir(local_path):
                if not local_path.endswith("/"):
                    local_path += "/"
                if not remote_path.endswith("/"):
                    remote_path += "/"
            self.ssh_cmd(
                external_ip,
                "mkdir -p {}".format(os.path.dirname(remote_path)))
            subprocess.check_call([
                "rsync", "-e", "ssh -i ~/.ssh/ekl-laptop-thinkpad.pem "
                "-o ConnectTimeout=60s -o StrictHostKeyChecking=no",
                "--delete", "-avz", "{}".format(local_path),
                "ubuntu@{}:{}".format(external_ip, remote_path)
            ], stdout=self.stdout, stderr=self.stderr)
        self.provider.set_node_tags(
            self.node_id, {TAG_RAY_WORKER_STATUS: "RunningInitCmds"})
        for cmd in self.init_cmds:
            self.ssh_cmd(external_ip, cmd)

    def ssh_cmd(self, ip, cmd, connect_timeout=60):
        subprocess.check_call([
            "ssh", "-o", "ConnectTimeout={}s".format(connect_timeout),
            "-o", "StrictHostKeyChecking=no",
            "-i", "~/.ssh/ekl-laptop-thinkpad.pem",
            "ubuntu@{}".format(ip),
            cmd,
        ], stdout=self.stdout, stderr=self.stderr)
