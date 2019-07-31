from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pytest
import subprocess

import ray


TEST_DIR = os.path.dirname(os.path.abspath(__file__))


def test_validation_success():
    project_files = [
        "docker_project.yaml",
        "requirements_project.yaml",
        "shell_project.yaml"
    ]
    for project_file in project_files:
        path = os.path.join(TEST_DIR, "project_files", project_file)
        ray.projects.validate_project(path)


def test_validation_failure():
    project_files = [
        "no_project1.yaml",
        "no_project2.yaml"
    ]
    for project_file in project_files:
        path = os.path.join(TEST_DIR, "project_files", project_file)
        with pytest.raises(Exception):
            ray.projects.validate_project(path)


def test_project_root():
    path = os.path.join(TEST_DIR, "project_files", "project1")
    assert ray.projects.find_root(path) == path

    path2 = os.path.join(TEST_DIR, "project_files", "project1", "subdir")
    assert ray.projects.find_root(path2) == path

    path3 = "/tmp/"
    assert ray.projects.find_root(path3) is None


def test_project_validation():
    path = os.path.join(TEST_DIR, "project_files", "project1")
    subprocess.check_call(["ray", "session", "create", "--dry"], cwd=path)
