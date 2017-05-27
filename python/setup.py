from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import shutil
import subprocess

from setuptools import setup, find_packages, Extension, Distribution
import setuptools.command.build_ext as _build_ext


class build_ext(_build_ext.build_ext):
  def run(self):
    subprocess.check_call(["../build.sh"])
    # Ideally, these files would already have been included because they're in
    # the MANIFEST.in, but the MANIFEST.in gets applied at the very beginning
    # when setup.py runs before these files have been created, so we have to
    # move the files manually.
    manifest_file = open("MANIFEST.in")
    filenames = [line.split(" ")[1].strip()
                 for line in manifest_file.readlines()]
    for filename in filenames:
      self.move_file(filename)

  def move_file(self, filename):
    # TODO(rkn): This feels very brittle. It may not handle all cases. See
    # https://github.com/apache/arrow/blob/master/python/setup.py for an
    # example.
    source = filename
    destination = os.path.join(self.build_lib, filename)
    # Create the target directory if it doesn't already exist.
    parent_directory = os.path.dirname(destination)
    if not os.path.exists(parent_directory):
      os.makedirs(parent_directory)
    print("Copying {} to {}.".format(source, destination))
    shutil.copyfile(source, destination)


class BinaryDistribution(Distribution):
  def has_ext_modules(self):
    return True


setup(name="ray",
      version="0.1.0",
      packages=find_packages(),
      # Dummy extension to trigger build_ext
      ext_modules=[Extension("__dummy__", sources=[])],
      cmdclass={"build_ext": build_ext},
      distclass=BinaryDistribution,
      install_requires=["numpy",
                        "funcsigs",
                        "colorama",
                        "psutil",
                        "redis",
                        "cloudpickle >= 0.2.2",
                        "flatbuffers"],
      include_package_data=True,
      zip_safe=False,
      license="Apache 2.0")
