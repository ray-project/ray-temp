"""
Some instructions on writing CLI tests:
1. Look at test_ray_start for a simple output test example.
2. To get a valid regex, start with copy-pasting your output from a captured
   version (no formatting). Then escape ALL regex characters (parenthesis,
   brackets, dots, etc.). THEN add ".+" to all the places where info might
   change run to run.
3. Look at test_ray_up for an example of how to mock AWS, commands,
   and autoscaler config.
4. Print your outputs!!!! Tests are impossible to debug if they fail
   and you did not print anything. Since command output is captured by click,
   MAKE SURE YOU print(result.output) when tests fail!!!

WARNING: IF YOU MOCK AWS, DON'T FORGET THE AWS_CREDENTIALS FIXTURE.
         THIS IS REQUIRED SO BOTO3 DOES NOT ACCESS THE ACTUAL AWS SERVERS.

Note: config cache does not work with AWS mocks since the AWS resource ids are
      randomized each time.

Note: while not strictly necessary for setup commands e.g. ray up,
      --log-new-style produces much cleaner output if the test fails.
"""

import sys
import re
import os
from contextlib import contextmanager
from pathlib import Path

import pytest

import moto
from moto import mock_ec2, mock_iam
from click.testing import CliRunner

from testfixtures import Replacer
from testfixtures.popen import MockPopen, PopenBehaviour

import ray.autoscaler.aws.config as aws_config
import ray.scripts.scripts as scripts


def _debug_die(result):
    print("!!!!")
    print(repr(result.output))
    print("!!!!")
    assert False


def _die_on_error(result):
    if result.exit_code == 0:
        return
    _debug_die(result)


def _debug_check_line_by_line(result, expected_lines):
    output_lines = result.output.split("\n")
    i = 0

    for out in output_lines:
        print(out)

        if i >= len(expected_lines):
            i += 1
            print("!!!!!! Expected fewer lines")
            continue

        exp = expected_lines[i]
        matched = re.fullmatch(exp + r" *", out) is not None
        if not matched:
            print("!!!!!!! Expected (regex):")
            print(repr(exp))
        i += 1
    while i < len(expected_lines):
        i += 1
        print("!!!!!!! Expected (regex):")
        print(repr(expected_lines[i]))

    assert False


def _setup_aws_mock():
    # moto (boto3 mock) only allows a hardcoded set of AMIs
    dlami = moto.ec2.ec2_backends["us-west-2"].describe_images(
        filters={"name": "Deep Learning AMI Ubuntu*"})[0].id
    aws_config.DEFAULT_AMI["us-west-2"] = dlami


@contextmanager
def _unlink_test_shh_key():
    """Use this to remove the keys spawned by ray up.
    """
    try:
        yield
    finally:
        try:
            Path("~", ".ssh", "__test-cli_key").unlink()
        except FileNotFoundError:
            pass


@contextmanager
def _setup_popen_mock(commands_mock):
    Popen = MockPopen()
    Popen.set_default(behaviour=commands_mock)

    with Replacer() as replacer:
        replacer.replace("subprocess.Popen", Popen)
        yield


def _get_pattern_path(name):
    p = Path(__file__).parent / "test_cli_patterns" / name
    return str(p)


def _load_output_pattern(name):
    with open(_get_pattern_path(name)) as f:
        # remove \n
        return [x[:-1] for x in f.readlines()]


def _check_output_via_pattern(name, result):
    expected_lines = _load_output_pattern(name)

    if result.exception is not None:
        print(result.output)
        raise result.exception from None

    expected = r" *\n".join(expected_lines) + "\n?"
    if re.fullmatch(expected, result.output) is None:
        _debug_check_line_by_line(result, expected_lines)

    assert result.exit_code == 0


default_test_config_path = (
    Path(__file__).parent / "test_cli_patterns" / "test_ray_up_config.json")


def test_ray_start():
    runner = CliRunner()
    result = runner.invoke(scripts.start, ["--head", "--log-new-style"])
    _die_on_error(runner.invoke(scripts.stop))

    _check_output_via_pattern("test_ray_start.txt", result)


@mock_ec2
@mock_iam
def test_ray_up(aws_credentials):
    _setup_aws_mock()

    # moto.settings.INITIAL_NO_AUTH_ACTION_COUNT = 0

    def commands_mock(command, stdin):
        # if we want to have e.g. some commands fail,
        # we can have overrides happen here.
        # unfortunately, cutting out SSH prefixes and such
        # is, to put it lightly, non-trivial
        if "uptime" in command:
            return PopenBehaviour(stdout="MOCKED uptime")
        if "rsync" in command:
            return PopenBehaviour(stdout="MOCKED rsync")
        if "ray" in command:
            return PopenBehaviour(stdout="MOCKED ray")
        return PopenBehaviour(stdout="MOCKED GENERIC")

    with _unlink_test_shh_key():
        with _setup_popen_mock(commands_mock):
            # expected_lines = _load_output_pattern("test_ray_up.txt")

            # config cache does not work with mocks
            runner = CliRunner()
            result = runner.invoke(scripts.up, [
                str(default_test_config_path), "--no-config-cache", "-y",
                "--log-new-style"
            ])

            _check_output_via_pattern("test_ray_up.txt", result)


@mock_ec2
@mock_iam
def test_ray_attach():
    _setup_aws_mock()

    def commands_mock(command, stdin):
        # TODO(maximsmol): this is a hack since stdout=sys.stdout
        #                  doesn't work with the mock for some reason
        print("ubuntu@ip-.+:~$ exit")
        return PopenBehaviour(stdout="ubuntu@ip-.+:~$ exit")

    with _unlink_test_shh_key():
        with _setup_popen_mock(commands_mock):
            runner = CliRunner()
            result = runner.invoke(scripts.up, [
                str(default_test_config_path), "--no-config-cache", "-y",
                "--log-new-style"
            ])
            _die_on_error(result)

            result = runner.invoke(
                scripts.attach,
                [str(default_test_config_path), "--log-new-style"])

            _check_output_via_pattern("test_ray_attach.txt", result)


@mock_ec2
@mock_iam
def test_ray_exec():
    _setup_aws_mock()

    def commands_mock(command, stdin):
        # TODO(maximsmol): this is a hack since stdout=sys.stdout
        #                  doesn't work with the mock for some reason
        print("This is a test!")
        return PopenBehaviour(stdout="This is a test!")

    with _unlink_test_shh_key():
        with _setup_popen_mock(commands_mock):
            runner = CliRunner()
            result = runner.invoke(scripts.up, [
                str(default_test_config_path), "--no-config-cache", "-y",
                "--log-new-style"
            ])
            _die_on_error(result)

            result = runner.invoke(scripts.exec, [
                str(default_test_config_path), "--log-new-style",
                "\"echo This is a test!\""
            ])

            _check_output_via_pattern("test_ray_exec.txt", result)


@mock_ec2
@mock_iam
def test_ray_submit():
    _setup_aws_mock()

    def commands_mock(command, stdin):
        # TODO(maximsmol): this is a hack since stdout=sys.stdout
        #                  doesn't work with the mock for some reason
        if "rsync" not in command:
            print("This is a test!")
        return PopenBehaviour(stdout="This is a test!")

    with _unlink_test_shh_key():
        with _setup_popen_mock(commands_mock):
            runner = CliRunner()
            result = runner.invoke(scripts.up, [
                str(default_test_config_path), "--no-config-cache", "-y",
                "--log-new-style"
            ])
            _die_on_error(result)

            result = runner.invoke(
                scripts.submit,
                [
                    str(default_test_config_path),
                    "--log-new-style",
                    # this is somewhat misleading, since the file
                    # actually never gets run
                    # TODO(maximsmol): make this work properly one day?
                    _get_pattern_path("test.py")
                ])

            _check_output_via_pattern("test_ray_submit.txt", result)


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
