from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import logging
import time

import ray
from ray.streaming.config import Config
from ray.streaming.streaming import Environment, Conf

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser()
parser.add_argument("--input-file", required=True, help="the input text file")


# Test functions
def splitter(line):
    return line.split()


def filter_fn(word):
    if "f" in word:
        return True
    return False


if __name__ == "__main__":

    args = parser.parse_args()

    ray.init(local_mode=True)

    # A Ray streaming environment with the default configuration
    env = Environment(config=Conf(queue_type=Config.MEMORY_QUEUE))

    # Stream represents the ouput of the filter and
    # can be forked into other dataflows
    stream = env.read_text_file(args.input_file) \
                .shuffle() \
                .flat_map(splitter) \
                .inspect(lambda x: print("result", x))     # Prints the contents of the
    # stream to stdout
    start = time.time()
    env_handle = env.execute()
    ray.get(env_handle)  # Stay alive until execution finishes
    env.wait_finish()
    end = time.time()
    logger.info("Elapsed time: {} secs".format(end - start))
    logger.debug("Output stream id: {}".format(stream.id))
