from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import logging
try:  # py3
    from shlex import quote
except ImportError:  # py2
    from pipes import quote

logger = logging.getLogger(__name__)

STAGING_PATH = "~/ray_staging/"


def dockerize_if_needed(config):
    if "docker" not in config:
        return config

    docker_image = config["docker"].get("image")
    cname = config["docker"].get("container_name")
    run_options = config["docker"].get("run_options", [])

    head_docker_image = config["docker"].get("head_image", docker_image)
    head_run_options = config["docker"].get("head_run_options", [])

    worker_docker_image = config["docker"].get("worker_image", docker_image)
    worker_run_options = config["docker"].get("worker_run_options", [])

    ssh_user = config["auth"]["ssh_user"]
    if not docker_image and not (head_docker_image and worker_docker_image):
        if cname:
            logger.warning(
                "dockerize_if_needed: "
                "Container name given but no Docker image(s) - continuing...")
        return config
    else:
        assert cname, "Must provide container name!"

    head_docker_start = docker_start_cmds(ssh_user, head_docker_image,
                                          config["docker"]["mounts"], cname,
                                          run_options + head_run_options)

    worker_docker_start = docker_start_cmds(ssh_user, worker_docker_image,
                                            config["docker"]["mounts"], cname,
                                            run_options + worker_run_options)

    config["head_setup_commands"] = (head_docker_start + with_docker_exec(
        config["head_setup_commands"], container_name=cname))
    config["head_start_ray_commands"] = (
        docker_autoscaler_setup(cname) + with_docker_exec(
            config["head_start_ray_commands"], container_name=cname))

    config["worker_setup_commands"] = (worker_docker_start + with_docker_exec(
        config["worker_setup_commands"], container_name=cname))
    config["worker_start_ray_commands"] = with_docker_exec(
        config["worker_start_ray_commands"],
        container_name=cname,
        env_vars=["RAY_HEAD_IP"])

    return config


def with_docker_exec(cmds, container_name, env_vars=None):
    env_str = ""
    if env_vars:
        env_str = " ".join(
            ["-e {env}=${env}".format(env=env) for env in env_vars])
    return [
        "docker exec {} {} /bin/sh -c {} ".format(env_str, container_name,
                                                  quote(cmd)) for cmd in cmds
    ]


def aptwait_cmd():
    return ("while sudo fuser"
            " /var/{lib/{dpkg,apt/lists},cache/apt/archives}/lock"
            " >/dev/null 2>&1; "
            "do echo 'Waiting for release of dpkg/apt locks'; sleep 5; done")


def docker_start_cmds(user, image, mount, cname, user_options):
    cmds = []

    # create flags
    # ports for the redis, object manager, and tune client
    port_flags = " ".join([
        "-p {port}:{port}".format(port=port)
        for port in ["6379", "8076", "4321"]
    ])
    mount_flags = " ".join([
        "--mount type=bind,source={src},target={dest}".format(
            src=src, dest=dest) for dest, src in mount.items()
    ])

    # for click, used in ray cli
    env_vars = {"LC_ALL": "C.UTF-8", "LANG": "C.UTF-8"}
    env_flags = " ".join(
        ["-e {name}={val}".format(name=k, val=v) for k, v in env_vars.items()])

    user_options_str = " ".join(user_options)
    # docker run command
    docker_check = [
        "docker", "inspect", "-f", "'{{.State.Running}}'", cname, "||"
    ]
    docker_run = [
        "docker", "run", "--rm", "--name {}".format(cname), "-d", "-it",
        port_flags, mount_flags, env_flags, user_options_str, "--net=host",
        image, "bash"
    ]
    cmds.append(" ".join(docker_check + docker_run))

    return cmds


def docker_autoscaler_setup(cname):
    # needed because docker doesn't allow relative paths
    files = {
        os.path.basename(path): path
        for path in ["~/ray_bootstrap_config.yaml", "~/ray_bootstrap_key.pem"]
    }
    cmds = docker_cp_file_cmds(files, cname)
    for base_name, path in files.items():
        cmds.extend(
            with_docker_exec(
                ["cp {} {}".format("/" + base_name, path)],
                container_name=cname))
    return cmds


def docker_cp_file_cmds(files, cname):
    cmds = []
    for dst, src in files.items():
        cmds.append("docker cp {path} {cname}:{dpath}".format(
            path=src, dpath=dst, cname=cname))
    return cmds
