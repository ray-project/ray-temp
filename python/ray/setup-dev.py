#!/usr/bin/env python
"""This script allows you to develop RLlib without needing to compile Ray."""

import argparse
import click
import os
import shutil
import subprocess

import ray


def do_link(package, force=False, local_path=None):
    package_home = os.path.abspath(os.path.join(ray.__file__, f"../{package}"))
    # Infer local_path automatically.
    if local_path is None:
        local_path = f"../{package}"
    local_home = os.path.abspath(os.path.join(__file__, local_path))
    # If installed package dir does not exist, continue either way. We'll
    # remove it/create a link from there anyways.
    if not os.path.isdir(package_home):
        print(f"{package_home} does not exist. Continuing to link.")
    # Make sure the path we are linking to does exist.
    assert os.path.exists(local_home), local_home
    # Confirm with user.
    if not force and not click.confirm(
            f"This will replace:\n  {package_home}\nwith "
            f"a symlink to:\n  {local_home}",
            default=True):
        return

    # Windows: Create directory junction.
    if os.name == "nt":
        try:
            shutil.rmtree(package_home)
        except FileNotFoundError:
            pass
        except OSError:
            os.remove(package_home)
        subprocess.check_call(
            ["mklink", "/J", package_home, local_home], shell=True)
    # Posix: Use `ln -s` to create softlink.
    else:
        sudo = []
        if not os.access(os.path.dirname(package_home), os.W_OK):
            print("You don't have write permission "
                  f"to {package_home}, using sudo:")
            sudo = ["sudo"]
        subprocess.check_call(sudo + ["rm", "-rf", package_home])
        subprocess.check_call(sudo + ["ln", "-s", local_home, package_home])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Setup dev.")
    parser.add_argument(
        "--yes", "-y", action="store_true", help="Don't ask for confirmation.")
    args = parser.parse_args()

    do_link("rllib", force=args.yes, local_path="../../../rllib")
    do_link("tune", force=args.yes)
    do_link("autoscaler", force=args.yes)
    do_link("scripts", force=args.yes)
    do_link("internal", force=args.yes)
    do_link("tests", force=args.yes)
    do_link("experimental", force=args.yes)
    do_link("util", force=args.yes)
    do_link("dashboard", force=args.yes)
    # Link package's `new_dashboard` directly to local (repo's) dashboard.
    # The repo's `new_dashboard` is a file, soft-linking to which will not work
    # on Mac.
    do_link("new_dashboard", force=args.yes, local_path="../../../dashboard")
    print("Created links.\n\nIf you run into issues initializing Ray, please "
          "ensure that your local repo and the installed Ray are in sync "
          "(pip install -U the latest wheels at "
          "https://docs.ray.io/en/master/installation.html, "
          "and ensure you are up-to-date on the master branch on git).\n\n"
          "Note that you may need to delete the package symlinks when pip "
          "installing new Ray versions to prevent pip from overwriting files "
          "in your git repo.")
