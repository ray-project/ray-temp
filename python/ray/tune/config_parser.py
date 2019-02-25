from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import json
import os

# For compatibility under py2 to consider unicode as str
from six import string_types

from ray.tune import TuneError
from ray.tune.result import DEFAULT_RESULTS_DIR
from ray.tune.trial import Trial, json_to_resources
from ray.tune.logger import _SafeFallbackEncoder, DEFAULT_LOGGERS


def make_parser(parser_creator=None, **kwargs):
    """Returns a base argument parser for the ray.tune tool.

    Args:
        parser_creator: A constructor for the parser class.
        kwargs: Non-positional args to be passed into the
            parser class constructor.
    """

    if parser_creator:
        parser = parser_creator(**kwargs)
    else:
        parser = argparse.ArgumentParser(**kwargs)

    # Note: keep this in sync with rllib/train.py
    parser.add_argument(
        "--run",
        default=None,
        type=str,
        help="The algorithm or model to train. This may refer to the name "
        "of a built-on algorithm (e.g. RLLib's DQN or PPO), or a "
        "user-defined trainable function or class registered in the "
        "tune registry.")
    parser.add_argument(
        "--stop",
        default="{}",
        type=json.loads,
        help="The stopping criteria, specified in JSON. The keys may be any "
        "field returned by 'train()' e.g. "
        "'{\"time_total_s\": 600, \"training_iteration\": 100000}' to stop "
        "after 600 seconds or 100k iterations, whichever is reached first.")
    parser.add_argument(
        "--config",
        default="{}",
        type=json.loads,
        help="Algorithm-specific configuration (e.g. env, hyperparams), "
        "specified in JSON.")
    parser.add_argument(
        "--resources-per-trial",
        default=None,
        type=json_to_resources,
        help="Override the machine resources to allocate per trial, e.g. "
        "'{\"cpu\": 64, \"gpu\": 8}'. Note that GPUs will not be assigned "
        "unless you specify them here. For RLlib, you probably want to "
        "leave this alone and use RLlib configs to control parallelism.")
    parser.add_argument(
        "--num-samples",
        default=1,
        type=int,
        help="Number of times to repeat each trial.")
    parser.add_argument(
        "--local-dir",
        default=DEFAULT_RESULTS_DIR,
        type=str,
        help="Local dir to save training results to. Defaults to '{}'.".format(
            DEFAULT_RESULTS_DIR))
    parser.add_argument(
        "--upload-dir",
        default="",
        type=str,
        help="Optional URI to sync training results to (e.g. s3://bucket).")
    parser.add_argument(
        "--trial-name-creator",
        default=None,
        help="Optional creator function for the trial string, used in "
        "generating a trial directory.")
    parser.add_argument(
        "--sync-function",
        default=None,
        help="Function for syncing the local_dir to upload_dir. If string, "
        "then it must be a string template for syncer to run and needs to "
        "include replacement fields '{local_dir}' and '{remote_dir}'.")
    parser.add_argument(
        "--loggers",
        default=None,
        help="List of logger creators to be used with each Trial. "
        "Defaults to ray.tune.logger.DEFAULT_LOGGERS.")
    parser.add_argument(
        "--checkpoint-freq",
        default=0,
        type=int,
        help="How many training iterations between checkpoints. "
        "A value of 0 (default) disables checkpointing.")
    parser.add_argument(
        "--checkpoint-at-end",
        action="store_true",
        help="Whether to checkpoint at the end of the experiment. "
        "Default is False.")
    parser.add_argument(
        "--export-formats",
        default=None,
        help="List of formats that exported at the end of the experiment. "
        "Default is None. For RLlib, 'checkpoint' and 'model' are "
        "supported for TensorFlow policy graphs.")
    parser.add_argument(
        "--max-failures",
        default=3,
        type=int,
        help="Try to recover a trial from its last checkpoint at least this "
        "many times. Only applies if checkpointing is enabled.")
    parser.add_argument(
        "--scheduler",
        default="FIFO",
        type=str,
        help="FIFO (default), MedianStopping, AsyncHyperBand, "
        "HyperBand, or HyperOpt.")
    parser.add_argument(
        "--scheduler-config",
        default="{}",
        type=json.loads,
        help="Config options to pass to the scheduler.")

    # Note: this currently only makes sense when running a single trial
    parser.add_argument(
        "--restore",
        default=None,
        type=str,
        help="If specified, restore from this checkpoint.")

    return parser


def to_argv(config):
    """Converts configuration to a command line argument format."""
    argv = []
    for k, v in config.items():
        if "-" in k:
            raise ValueError("Use '_' instead of '-' in `{}`".format(k))
        if not isinstance(v, bool) or v:  # for argparse flags
            argv.append("--{}".format(k.replace("_", "-")))
        if isinstance(v, string_types):
            argv.append(v)
        elif isinstance(v, bool):
            pass
        else:
            argv.append(json.dumps(v, cls=_SafeFallbackEncoder))
    return argv


def create_trial_from_spec(spec, output_path, parser, **trial_kwargs):
    """Creates a Trial object from parsing the spec.

    Arguments:
        spec (dict): A resolved experiment specification. Arguments should
            The args here should correspond to the command line flags
            in ray.tune.config_parser.
        output_path (str); A specific output path within the local_dir.
            Typically the name of the experiment.
        parser (ArgumentParser): An argument parser object from
            make_parser.
        trial_kwargs: Extra keyword arguments used in instantiating the Trial.

    Returns:
        A trial object with corresponding parameters to the specification.
    """
    try:
        args = parser.parse_args(to_argv(spec))
    except SystemExit:
        raise TuneError("Error parsing args, see above message", spec)
    if "resources_per_trial" in spec:
        trial_kwargs["resources"] = json_to_resources(
            spec["resources_per_trial"])
    return Trial(
        # Submitting trial via server in py2.7 creates Unicode, which does not
        # convert to string in a straightforward manner.
        trainable_name=spec["run"],
        # json.load leads to str -> unicode in py2.7
        config=spec.get("config", {}),
        local_dir=os.path.join(args.local_dir, output_path),
        # json.load leads to str -> unicode in py2.7
        stopping_criterion=spec.get("stop", {}),
        checkpoint_freq=args.checkpoint_freq,
        checkpoint_at_end=args.checkpoint_at_end,
        export_formats=spec.get("export_formats", []),
        # str(None) doesn't create None
        restore_path=spec.get("restore"),
        upload_dir=args.upload_dir,
        trial_name_creator=spec.get("trial_name_creator"),
        loggers=spec.get("loggers", DEFAULT_LOGGERS),
        # str(None) doesn't create None
        sync_function=spec.get("sync_function"),
        max_failures=args.max_failures,
        **trial_kwargs)
