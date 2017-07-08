from collections import namedtuple
from datetime import datetime
import logging
import os
import simplejson
import sys
import tempfile
import uuid
try:
  import smart_open
except ImportError:
  print("Cannot import smart_open, which means writing results "
        "to S3 will not be possible. Run 'pip install smart_open' "
        "if you want to use the --s3-bucket flag.")
if sys.version_info[0] == 2:
  import cStringIO as StringIO
elif sys.version_info[0] == 3:
  import io as StringIO

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class S3Logger(object):
  """Writing small amounts of data to S3 with real-time updates.
  """

  def __init__(self, uri):
    self.result_buffer = StringIO.StringIO()
    self.uri = uri

  def write(self, b):
    # TODO(pcm): At the moment we are writing the whole results output from
    # the beginning in each iteration. This will write O(n^2) bytes where n
    # is the number of bytes printed so far. Fix this! This should at least
    # only write the last 5MBs (S3 chunksize).
    with smart_open.smart_open(self.uri, "wb") as f:
      self.result_buffer.write(b)
      f.write(self.result_buffer.getvalue())


TrainingResult = namedtuple("TrainingResult", [
    "experiment_id",
    "training_iteration",
    "episode_reward_mean",
    "episode_len_mean",
])


class Algorithm(object):
  """All RLlib algorithms extend this base class.

  Algorithm objects retain internal model state between calls to train(), so
  you should create a new algorithm instance for each training session.

  Attributes:
    env_name (str): Name of the OpenAI gym environment to train against.
    config (obj): Algorithm-specific configuration data.
    logdir (str): Directory in which training outputs should be placed.

  TODO(ekl): support checkpoint / restore of training state.
  """

  def __init__(self, env_name, config, s3_bucket=None):
    self.experiment_id = uuid.uuid4()
    self.env_name = env_name
    self.config = config
    self.config.update({"experiment_id": self.experiment_id.hex})
    self.config.update({"env_name": env_name})
    self.logprefix = "{}_{}_{}".format(
        env_name,
        self.__class__.__name__,
        datetime.today().strftime("%Y-%m-%d_%H-%M-%S"))
    self.logdir = tempfile.mkdtemp(prefix=self.logprefix, dir="/tmp/ray")
    if s3_bucket:
      s3_path = s3_bucket + "/" + self.logprefix + "/" + "config.json"
      with smart_open.smart_open(s3_path, "wb") as f:
        simplejson.dump(self.config, f, sort_keys=True, ignore_nan=True)
    simplejson.dump(
        self.config, open(os.path.join(self.logdir, "config.json"), "w"),
        sort_keys=True, indent=4)
    logger.info(
        "%s algorithm created with logdir '%s'",
        self.__class__.__name__, self.logdir)

  def train(self):
    """Runs one logical iteration of training.

    Returns:
      A TrainingResult that describes training progress.
    """

    raise NotImplementedError
