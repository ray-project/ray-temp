from itertools import chain
import os
import re
import shutil
import subprocess
import sys

from setuptools import setup, find_packages, Distribution
import setuptools.command.build_ext as _build_ext

# Ideally, we could include these files by putting them in a
# MANIFEST.in or using the package_data argument to setup, but the
# MANIFEST.in gets applied at the very beginning when setup.py runs
# before these files have been created, so we have to move the files
# manually.

# NOTE: The lists below must be kept in sync with ray/BUILD.bazel.
ray_files = [
    "ray/core/src/ray/thirdparty/redis/src/redis-server",
    "ray/core/src/ray/gcs/redis_module/libray_redis_module.so",
    "ray/core/src/plasma/plasma_store_server",
    "ray/_raylet.so",
    "ray/core/src/ray/raylet/raylet_monitor",
    "ray/core/src/ray/gcs/gcs_server",
    "ray/core/src/ray/raylet/raylet",
    "ray/dashboard/dashboard.py",
    "ray/streaming/_streaming.so",
]

build_java = os.getenv("RAY_INSTALL_JAVA") == "1"
if build_java:
    ray_files.append("ray/jars/ray_dist.jar")

# These are the directories where automatically generated Python protobuf
# bindings are created.
generated_python_directories = [
    "ray/core/generated",
    "ray/streaming/generated",
]

optional_ray_files = []

ray_autoscaler_files = [
    "ray/autoscaler/aws/example-full.yaml",
    "ray/autoscaler/gcp/example-full.yaml",
    "ray/autoscaler/local/example-full.yaml",
    "ray/autoscaler/kubernetes/example-full.yaml",
    "ray/autoscaler/kubernetes/kubectl-rsync.sh",
]

ray_project_files = [
    "ray/projects/schema.json", "ray/projects/templates/cluster_template.yaml",
    "ray/projects/templates/project_template.yaml",
    "ray/projects/templates/requirements.txt"
]

ray_dashboard_files = [
    os.path.join(dirpath, filename)
    for dirpath, dirnames, filenames in os.walk("ray/dashboard/client/build")
    for filename in filenames
]

optional_ray_files += ray_autoscaler_files
optional_ray_files += ray_project_files
optional_ray_files += ray_dashboard_files

if "RAY_USE_NEW_GCS" in os.environ and os.environ["RAY_USE_NEW_GCS"] == "on":
    ray_files += [
        "ray/core/src/credis/build/src/libmember.so",
        "ray/core/src/credis/build/src/libmaster.so",
        "ray/core/src/credis/redis/src/redis-server"
    ]

extras = {
    "debug": [],
    "dashboard": [],
    "serve": ["uvicorn", "pygments", "werkzeug", "flask", "pandas", "blist"],
    "tune": ["tabulate", "tensorboardX"],
}

extras["rllib"] = extras["tune"] + [
    "pyyaml",
    "gym[atari]",
    "opencv-python-headless",
    "lz4",
    "scipy",
]

extras["all"] = list(set(chain.from_iterable(extras.values())))


class build_ext(_build_ext.build_ext):
    def run(self):
        # Note: We are passing in sys.executable so that we use the same
        # version of Python to build packages inside the build.sh script. Note
        # that certain flags will not be passed along such as --user or sudo.
        # TODO(rkn): Fix this.
        command = ["../build.sh", "-p", sys.executable]
        if build_java:
            # Also build binaries for Java if the above env variable exists.
            command += ["-l", "python,java"]
        subprocess.check_call(command)

        # We also need to install pickle5 along with Ray, so make sure that the
        # relevant non-Python pickle5 files get copied.
        pickle5_files = self.walk_directory("./ray/pickle5_files/pickle5")

        thirdparty_files = self.walk_directory("./ray/thirdparty_files")

        files_to_include = ray_files + pickle5_files + thirdparty_files

        # Copy over the autogenerated protobuf Python bindings.
        for directory in generated_python_directories:
            for filename in os.listdir(directory):
                if filename[-3:] == ".py":
                    files_to_include.append(os.path.join(directory, filename))

        for filename in files_to_include:
            self.move_file(filename)

        # Try to copy over the optional files.
        for filename in optional_ray_files:
            try:
                self.move_file(filename)
            except Exception:
                print("Failed to copy optional file {}. This is ok."
                      .format(filename))

    def walk_directory(self, directory):
        file_list = []
        for (root, dirs, filenames) in os.walk(directory):
            for name in filenames:
                file_list.append(os.path.join(root, name))
        return file_list

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
        if not os.path.exists(destination):
            print("Copying {} to {}.".format(source, destination))
            shutil.copy(source, destination, follow_symlinks=True)


class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True


def find_version(*filepath):
    # Extract version information from filepath
    here = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(here, *filepath)) as fp:
        version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                                  fp.read(), re.M)
        if version_match:
            return version_match.group(1)
        raise RuntimeError("Unable to find version string.")


requires = [
    "numpy >= 1.16",
    "filelock",
    "jsonschema",
    "funcsigs",
    "click",
    "colorama",
    "packaging",
    "pytest",
    "pyyaml",
    "redis>=3.3.2",
    # NOTE: Don't upgrade the version of six! Doing so causes installation
    # problems. See https://github.com/ray-project/ray/issues/4169.
    "six >= 1.0.0",
    "faulthandler;python_version<'3.3'",
    "protobuf >= 3.8.0",
    "cloudpickle",
    "py-spy >= 0.2.0",
    "aiohttp",
    "google",
    "grpcio"
]

setup(
    name="ray",
    version=find_version("ray", "__init__.py"),
    author="Ray Team",
    author_email="ray-dev@googlegroups.com",
    description=("A system for parallel and distributed Python that unifies "
                 "the ML ecosystem."),
    long_description=open("../README.rst").read(),
    url="https://github.com/ray-project/ray",
    keywords=("ray distributed parallel machine-learning "
              "reinforcement-learning deep-learning python"),
    packages=find_packages(),
    cmdclass={"build_ext": build_ext},
    # The BinaryDistribution argument triggers build_ext.
    distclass=BinaryDistribution,
    install_requires=requires,
    setup_requires=["cython >= 0.29"],
    extras_require=extras,
    entry_points={
        "console_scripts": [
            "ray=ray.scripts.scripts:main",
            "rllib=ray.rllib.scripts:cli [rllib]", "tune=ray.tune.scripts:cli"
        ]
    },
    include_package_data=True,
    zip_safe=False,
    license="Apache 2.0")
