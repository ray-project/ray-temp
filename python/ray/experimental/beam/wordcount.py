#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""A word-counting workflow."""

# pytype: skip-file

from __future__ import absolute_import

import argparse
import logging
import re

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from runner import RayWorkerEnvironment

import ray
ray.init()

NUM_WORKERS = ray.available_resources()["CPU"]


class WordExtractingDoFn(beam.DoFn):
    """Parse each line of input text into words."""

    def process(self, element):
        """Returns an iterator over the words of this element.

    The element is a line of text.  If the line is blank, note that, too.

    Args:
      element: the element being processed

    Returns:
      The processed element.
    """
        import time
        start = time.time()
        while time.time() - start < 0.1:
            pass
        import re
        return re.findall(r'[\w\']+', element, re.UNICODE)


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='test_in.txt',
        help='Input file to process.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.append('--runner=ray.experimental.beam.runner.RayRunner')
    pipeline_args.append('--direct_num_workers={}'.format(int(NUM_WORKERS)))
    #    pipeline_args.append('--direct_running_mode=multi_processing')
    #pipeline_args.append('--runner=BundleBasedDirectRunner')

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)

    from apache_beam.portability.api import beam_runner_api_pb2
    from apache_beam.runners.portability.fn_api_runner import FnApiRunner
    from apache_beam.portability import python_urns
    import sys

    #    env = beam_runner_api_pb2.Environment(urn="ray_worker")
    #    print(env)
    #    runner = FnApiRunner(default_environment=env)
    cmd = 'python -m apache_beam.runners.worker.sdk_worker_main'

    runner = FnApiRunner(default_environment=RayWorkerEnvironment(cmd))

    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session

    # The pipeline will be run on exiting the with block.
    with beam.Pipeline(options=pipeline_options, runner=runner) as p:

        # Read the text file[pattern] into a PCollection.
        #lines = p | 'Read' >> ReadFromText(known_args.input)
        #lines = p | 'Read' >> ReadFromText("s3://apache-beam-test/input/wordcount.txt")
        lines = p | 'Read' >> ReadFromText("s3://apache-beam-test/input/")

        counts = (lines
                  | 'Split' >>
                  (beam.ParDo(WordExtractingDoFn()).with_output_types(unicode))
                  | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
                  | 'GroupAndSum' >> beam.CombinePerKey(sum))

        # Format the counts into a PCollection of strings.
        def format_result(word, count):
            return '%s: %d' % (word, count)

        output = counts | 'Format' >> beam.MapTuple(format_result)
        #output | 'Write' >> WriteToText("output")
        output | 'Write' >> WriteToText("s3://apache-beam-test/output")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
