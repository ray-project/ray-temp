import os
import logging
try:  # py3
    from shlex import quote
except ImportError:  # py2
    from pipes import quote

logger = logging.getLogger(__name__)


def validate_docker_config(config):
    if "docker" not in config:
        return config

    docker_image = config["docker"].get("image")
    cname = config["docker"].get("container_name")

    head_docker_image = config["docker"].get("head_image", docker_image)

    worker_docker_image = config["docker"].get("worker_image", docker_image)

    image_present = docker_image or (head_docker_image and worker_docker_image)
    if (not cname) and (not image_present):
        return
    else:
        assert cname and image_present, "Must provide a container & image name"


    return config


def with_docker_exec(cmds,
                     container_name,
                     env_vars=None,
                     with_interactive=False):
    env_str = ""
    if env_vars:
        env_str = " ".join(
            ["-e {env}=${env}".format(env=env) for env in env_vars])
    return [
        "docker exec {interactive} {env} {container} /bin/bash -c {cmd} ".
        format(
            interactive="-it" if with_interactive else "",
            env=env_str,
            container=container_name,
            cmd=quote(cmd)) for cmd in cmds
    ]


def check_docker_running_cmd(cname):
    return " ".join([
        "docker", "inspect", "-f", "'{{.State.Running}}'", cname, "||", "true"
    ])


def check_docker_image(cname):
    return " ".join([
        "docker", "inspect", "-f", "'{{.Config.Image}}'", cname, "||", "true"
    ])


def docker_start_cmds(user, image, mount_dict, cname, user_options):
    mount = {dst: dst for dst in mount_dict}

    # TODO(ilr) Move away from defaulting to /root/
    mount_flags = " ".join([
        "-v {src}:{dest}".format(src=k, dest=v.replace("~/", "/root/"))
        for k, v in mount.items()
    ])

    # for click, used in ray cli
    env_vars = {"LC_ALL": "C.UTF-8", "LANG": "C.UTF-8"}
    env_flags = " ".join(
        ["-e {name}={val}".format(name=k, val=v) for k, v in env_vars.items()])

    user_options_str = " ".join(user_options)
    docker_run = [
        "docker", "run", "--rm", "--name {}".format(cname), "-d", "-it",
        mount_flags, env_flags, user_options_str, "--net=host", image, "bash"
    ]
    return " ".join(docker_run)


def docker_autoscaler_setup(cname):
    cmds = []
    for path in ["~/ray_bootstrap_config.yaml", "~/ray_bootstrap_key.pem"]:
        # needed because docker doesn't allow relative paths
        base_path = os.path.basename(path)
        cmds.append("docker cp {path} {cname}:{dpath}".format(
            path=path, dpath=base_path, cname=cname))
        cmds.extend(
            with_docker_exec(
                ["cp {} {}".format("/" + base_path, path)],
                container_name=cname))
    return cmds
