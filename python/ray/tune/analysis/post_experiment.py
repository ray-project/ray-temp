from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import logging
import os.path as osp
import numpy as np
import pandas as pd
from pandas.api.types import is_string_dtype, is_numeric_dtype
from ray.tune.util import flatten_dict

logger = logging.getLogger(__name__)


def get_best_trial(trial_list, metric):
    """Retrieve the best trial."""
    return max(trial_list, key=lambda trial: trial.last_result.get(metric, 0))

def get_sorted_trials(trial_list, metric):
    return sorted(trial_list, key=lambda trial: trial.last_result.get(metric, 0), reverse=True)

def get_best_result(trial_list, metric):
    """Retrieve the last result from the best trial."""
    return {metric: get_best_trial(trial_list, metric).last_result[metric]}


def _parse_results(res_path):
    res_dict = {}
    try:
        with open(res_path) as f:
            # Get last line in file
            for line in f:
                pass
        res_dict = flatten_dict(json.loads(line.strip()))
    except Exception:
        logger.exception("Importing %s failed...Perhaps empty?" % res_path)
    return res_dict

def _parse_configs(cfg_path):
    try:
        with open(cfg_path) as f:
            cfg_dict = flatten_dict(json.load(f))
    except Exception:
        logger.exception("Config parsing failed.")
    return cfg_dict

def _resolve(directory, result_fname):
    try:
        resultp = osp.join(directory, result_fname)
        res_dict = _parse_results(resultp)
        cfgp = osp.join(directory, "params.json")
        cfg_dict = _parse_configs(cfgp)
        cfg_dict.update(res_dict)
        return cfg_dict
    except Exception:
        return None

def load_results_to_df(directory, result_name="result.json"):
    """ Loads results to pandas dataframe """
    exp_directories = [
        dirpath for dirpath, dirs, files in os.walk(directory) for f in files
        if f == result_name
    ]
    data = [_resolve(d, result_name) for d in exp_directories]
    data = [d for d in data if d]
    return pd.DataFrame(data)

def generate_plotly_dim_dict(df, field):
    dim_dict = {}
    dim_dict["label"] = field
    column = df[field]
    if is_numeric_dtype(column):
        dim_dict["values"] = column
    elif is_string_dtype(column):
        texts = column.unique()
        dim_dict["values"] = [
            np.argwhere(texts == x).flatten()[0] for x in column
        ]
        dim_dict["tickvals"] = list(range(len(texts)))
        dim_dict["ticktext"] = texts
    else:
        raise Exception("Unidentifiable Type")

    return dim_dict