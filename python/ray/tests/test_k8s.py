import os
import subprocess
import time
import unittest

import pytest

from ray.autoscaler._private.kubernetes import core_api


class KubernetesTest(unittest.TestCase):
    def test_up_and_down(self):
        """(1) Runs 'ray up' with a Kubernetes config that specifies
        min_workers=1.
        (2) Runs 'ray exec' to read monitor logs and confirm that worker and
        head are connected.
        (3) Runs 'ray down' and confirms that the cluster is gone."""
        with open("k8s_test.yaml", "w") as f:
            f.write(test_yaml)

        # ray up
        os.system(
            "minikube start; ray up k8s_test.yaml --yes --no-config-cache")

        # Check for two pods (worker and head).
        while True:
            pod_list = core_api().list_namespaced_pod(namespace="ray")
            if len(pod_list.items) == 2:
                print("Head and worker pods started.")
                break
            else:
                print("Waiting for worker pod to start.")
                time.sleep(1)

        # Read logs with ray exec and check that worker and head are connected.
        # (Since the config yaml is legacy-style, we check for
        # ray-legacy-*-node_type.)
        while True:
            monitor_output = subprocess.check_output([
                "ray", "exec", "k8s_test.yaml",
                "tail -n 100 /tmp/ray/session_latest/logs/monitor*"
            ]).decode()
            output_ok = ("ray-legacy-head-node-type" in monitor_output
                         and "ray-legacy-worker-node-type" in monitor_output)
            if output_ok:
                print("Worker node connected. Monitoring ok.")
                break
            else:
                print("Waiting for worker to connect.")
                time.sleep(1)

        # ray down.
        os.system("ray down k8s_test.yaml --yes")

        # Check that there are no pods left in namespace ray to confirm that
        # the cluster is gone.
        while True:
            pod_list = core_api().list_namespaced_pod(namespace="ray")
            if len(pod_list.items) == 0:
                print("Cluster shut down succesfully.")
                break
            else:
                print("Waiting for cluster to shut down.")

        os.remove("k8s_test.yaml")

test_yaml = """
# An unique identifier for the head node and workers of this cluster.
cluster_name: default

# The minimum number of workers nodes to launch in addition to the head
# node. This number should be >= 0.
min_workers: 1

# The maximum number of workers nodes to launch in addition to the head
# node. This takes precedence over min_workers.
max_workers: 2

# The autoscaler will scale up the cluster faster with higher upscaling speed.
# E.g., if the task requires adding more nodes then autoscaler will gradually
# scale up the cluster in chunks of upscaling_speed*currently_running_nodes.
# This number should be > 0.
upscaling_speed: 1.0

# If a node is idle for this many minutes, it will be removed.
idle_timeout_minutes: 5

# Kubernetes resources that need to be configured for the autoscaler to be
# able to manage the Ray cluster. If any of the provided resources don't
# exist, the autoscaler will attempt to create them. If this fails, you may
# not have the required permissions and will have to request them to be
# created by your cluster administrator.
provider:
    type: kubernetes

    # Exposing external IP addresses for ray pods isn't currently supported.
    use_internal_ips: true

    # Namespace to use for all resources created.
    namespace: ray

    # ServiceAccount created by the autoscaler for the head node pod that it
    # runs in. If this field isn't provided, the head pod config below must
    # contain a user-created service account with the proper permissions.
    autoscaler_service_account:
        apiVersion: v1
        kind: ServiceAccount
        metadata:
            name: autoscaler

    # Role created by the autoscaler for the head node pod that it runs in.
    # If this field isn't provided, the role referenced in
    # autoscaler_role_binding must exist and have at least these permissions.
    autoscaler_role:
        kind: Role
        apiVersion: rbac.authorization.k8s.io/v1
        metadata:
            name: autoscaler
        rules:
        - apiGroups: [""]
          resources: ["pods", "pods/status", "pods/exec"]
          verbs: ["get", "watch", "list", "create", "delete", "patch"]

    # RoleBinding created by the autoscaler for the head node pod that it runs
    # in. If this field isn't provided, the head pod config below must contain
    # a user-created service account with the proper permissions.
    autoscaler_role_binding:
        apiVersion: rbac.authorization.k8s.io/v1
        kind: RoleBinding
        metadata:
            name: autoscaler
        subjects:
        - kind: ServiceAccount
          name: autoscaler
        roleRef:
            kind: Role
            name: autoscaler
            apiGroup: rbac.authorization.k8s.io

    services:
      # Service that maps to the head node of the Ray cluster.
      - apiVersion: v1
        kind: Service
        metadata:
            # NOTE: If you're running multiple Ray clusters with services
            # on one Kubernetes cluster, they must have unique service
            # names.
            name: ray-head
        spec:
            # This selector must match the head node pod's selector below.
            selector:
                component: ray-head
            ports:
                - protocol: TCP
                  port: 8000
                  targetPort: 8000

      # Service that maps to the worker nodes of the Ray cluster.
      - apiVersion: v1
        kind: Service
        metadata:
            # NOTE: If you're running multiple Ray clusters with services
            # on one Kubernetes cluster, they must have unique service
            # names.
            name: ray-workers
        spec:
            # This selector must match the worker node pods' selector below.
            selector:
                component: ray-worker
            ports:
                - protocol: TCP
                  port: 8000
                  targetPort: 8000

# Kubernetes pod config for the head node pod.
head_node:
    apiVersion: v1
    kind: Pod
    metadata:
        # Automatically generates a name for the pod with this prefix.
        generateName: ray-head-

        # Must match the head node service selector above if a head node
        # service is required.
        labels:
            component: ray-head
    spec:
        # Change this if you altered the autoscaler_service_account above
        # or want to provide your own.
        serviceAccountName: autoscaler

        # Restarting the head node automatically is not currently supported.
        # If the head node goes down, `ray up` must be run again.
        restartPolicy: Never

        # This volume allocates shared memory for Ray to use for its plasma
        # object store. If you do not provide this, Ray will fall back to
        # /tmp which cause slowdowns if is not a shared memory volume.
        volumes:
        - name: dshm
          emptyDir:
              medium: Memory

        containers:
        - name: ray-node
          imagePullPolicy: Never
          # You are free (and encouraged) to use your own container image,
          # but it should have the following installed:
          #   - rsync (used for `ray rsync` commands and file mounts)
          #   - screen (used for `ray attach`)
          #   - kubectl (used by the autoscaler to manage worker pods)
          image: rayproject/ray-test
          # Do not change this command - it keeps the pod alive until it is
          # explicitly killed.
          command: ["/bin/bash", "-c", "--"]
          args: ["trap : TERM INT; sleep infinity & wait;"]
          ports:
              - containerPort: 6379 # Redis port.
              - containerPort: 6380 # Redis port.
              - containerPort: 6381 # Redis port.
              - containerPort: 12345 # Ray internal communication.
              - containerPort: 12346 # Ray internal communication.

          # This volume allocates shared memory for Ray to use for its plasma
          # object store. If you do not provide this, Ray will fall back to
          # /tmp which cause slowdowns if is not a shared memory volume.
          volumeMounts:
              - mountPath: /dev/shm
                name: dshm
          resources:
              requests:
                  cpu: 100m
                  memory: 512Mi
              limits:
                  # The maximum memory that this pod is allowed to use. The
                  # limit will be detected by ray and split to use 10% for
                  # redis, 30% for the shared memory object store, and the
                  # rest for application memory. If this limit is not set and
                  # the object store size is not set manually, ray will
                  # allocate a very large object store in each pod that may
                  # cause problems for other pods.
                  memory: 2Gi
          env:
              # This is used in the head_start_ray_commands below so that
              # Ray can spawn the correct number of processes. Omitting this
              # may lead to degraded performance.
              - name: MY_CPU_REQUEST
                valueFrom:
                    resourceFieldRef:
                        resource: requests.cpu

# Kubernetes pod config for worker node pods.
worker_nodes:
    apiVersion: v1
    kind: Pod
    metadata:
        # Automatically generates a name for the pod with this prefix.
        generateName: ray-worker-

        # Must match the worker node service selector above if a worker node
        # service is required.
        labels:
            component: ray-worker
    spec:
        serviceAccountName: default

        # Worker nodes will be managed automatically by the head node, so
        # do not change the restart policy.
        restartPolicy: Never

        # This volume allocates shared memory for Ray to use for its plasma
        # object store. If you do not provide this, Ray will fall back to
        # /tmp which cause slowdowns if is not a shared memory volume.
        volumes:
        - name: dshm
          emptyDir:
              medium: Memory

        containers:
        - name: ray-node
          imagePullPolicy: Never
          # You are free (and encouraged) to use your own container image,
          # but it should have the following installed:
          #   - rsync (used for `ray rsync` commands and file mounts)
          image: rayproject/ray-test
          # Do not change this command - it keeps the pod alive until it is
          # explicitly killed.
          command: ["/bin/bash", "-c", "--"]
          args: ["trap : TERM INT; sleep infinity & wait;"]
          ports:
              - containerPort: 12345 # Ray internal communication.
              - containerPort: 12346 # Ray internal communication.

          # This volume allocates shared memory for Ray to use for its plasma
          # object store. If you do not provide this, Ray will fall back to
          # /tmp which cause slowdowns if is not a shared memory volume.
          volumeMounts:
              - mountPath: /dev/shm
                name: dshm
          resources:
              requests:
                  cpu: 100m
                  memory: 512Mi
              limits:
                  # This memory limit will be detected by ray and split into
                  # 30% for plasma, and 70% for workers.
                  memory: 2Gi
          env:
              # This is used in the head_start_ray_commands below so that
              # Ray can spawn the correct number of processes. Omitting this
              # may lead to degraded performance.
              - name: MY_CPU_REQUEST
                valueFrom:
                    resourceFieldRef:
                        resource: requests.cpu

# Files or directories to copy to the head and worker nodes. The format is a
# dictionary from REMOTE_PATH: LOCAL_PATH, e.g.
file_mounts: {
#    "/path1/on/remote/machine": "/path1/on/local/machine",
#    "/path2/on/remote/machine": "/path2/on/local/machine",
}

# Files or directories to copy from the head node to the worker nodes. The format is a
# list of paths. The same path on the head node will be copied to the worker node.
# This behavior is a subset of the file_mounts behavior. In the vast majority of cases
# you should just use file_mounts. Only use this if you know what you're doing!
cluster_synced_files: []

# Whether changes to directories in file_mounts or cluster_synced_files in the head node
# should sync to the worker node continuously
file_mounts_sync_continuously: False

# Patterns for files to exclude when running rsync up or rsync down.
# This is not supported on kubernetes.
# rsync_exclude: []

# Pattern files to use for filtering out files when running rsync up or rsync down. The file is searched for
# in the source directory and recursively through all subdirectories. For example, if .gitignore is provided
# as a value, the behavior will match git's behavior for finding and using .gitignore files.
# This is not supported on kubernetes.
# rsync_filter: []

# List of commands that will be run before `setup_commands`. If docker is
# enabled, these commands will run outside the container and before docker
# is setup.
initialization_commands: []

# List of shell commands to run to set up nodes.
setup_commands: []

# Custom commands that will be run on the head node after common setup.
head_setup_commands: []

# Custom commands that will be run on worker nodes after common setup.
worker_setup_commands: []

# Command to start ray on the head node. You don't need to change this.
# Note webui-host is set to 0.0.0.0 so that kubernetes can port forward.
head_start_ray_commands:
    - ray stop
    - ulimit -n 65536; ray start --head --num-cpus=$MY_CPU_REQUEST --port=6379 --object-manager-port=8076 --autoscaling-config=~/ray_bootstrap_config.yaml --dashboard-host 0.0.0.0

# Command to start ray on worker nodes. You don't need to change this.
worker_start_ray_commands:
    - ray stop
    - ulimit -n 65536; ray start --num-cpus=$MY_CPU_REQUEST --address=$RAY_HEAD_IP:6379 --object-manager-port=8076
""" # noqa

if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
