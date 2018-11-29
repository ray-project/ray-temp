from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from datetime import datetime
import json
import logging
import numpy as np
import os
import time

from ray.rllib.io.output_writer import OutputWriter
from ray.rllib.evaluation.sample_batch import SampleBatch
from ray.rllib.utils.compression import pack, unpack

logger = logging.getLogger(__name__)


class JsonWriter(OutputWriter):
    """Writer object that saves experiences in JSON file chunks."""

    def __init__(self,
                 ioctx,
                 path,
                 max_file_size=64000000,
                 compress_columns=frozenset(["obs", "new_obs"])):
        self.ioctx = ioctx
        self.path = path
        self.max_file_size = max_file_size
        self.compress_columns = compress_columns
        try:
            os.makedirs(path)
        except OSError:
            pass  # already exists
        assert os.path.exists(path), "Failed to create {}".format(path)
        self.file_index = 0
        self.bytes_written = 0
        self.cur_file = None

    def write(self, sample_batch):
        start = time.time()
        data = to_json(sample_batch, self.compress_columns)
        f = self._get_file()
        f.write(data)
        f.write("\n")
        f.flush()
        self.bytes_written += len(data)
        logger.debug("Wrote {} bytes to {} in {}s".format(
            len(data), f,
            time.time() - start))

    def _get_file(self):
        if not self.cur_file or self.bytes_written >= self.max_file_size:
            if self.cur_file:
                self.cur_file.close()
            timestr = datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
            self.cur_file = open(
                os.path.join(
                    self.path, "output-{}_worker-{}_{}.json".format(
                        timestr, self.ioctx.worker_index, self.file_index)),
                "w")
            self.file_index += 1
            logger.info("Writing to new output file {}".format(self.cur_file))
        return self.cur_file


def _to_jsonable(v, compress):
    if compress:
        return str(pack(v).decode("ascii"))
    elif isinstance(v, np.ndarray):
        return v.tolist()
    return v


def to_json(batch, compress_columns):
    return json.dumps({
        k: _to_jsonable(v, compress=k in compress_columns)
        for k, v in batch.data.items()
    })


def from_json(batch):
    data = json.loads(batch)
    for k, v in data.items():
        if type(v) is str:
            data[k] = unpack(v)
        else:
            data[k] = np.array(v)
    return SampleBatch(data)
