from setuptools import setup
import os
import re
import subprocess
import shutil

ray_java_py_pkg = "ray_java"
current_dir = os.path.abspath(os.path.dirname(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, os.pardir, os.pardir))
ray_java_build_dir = os.path.abspath(os.path.join(root_dir, "build", "java"))
ray_streaming_java_build_dir = os.path.abspath(
    os.path.join(root_dir, "streaming", "build", "java"))
jars_dir = os.path.join(current_dir, ray_java_py_pkg, "jars")


def find_version():
    version_file = os.path.join(root_dir, "python", "ray", "__init__.py")
    with open(version_file) as fp:
        version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                                  fp.read(), re.M)
        if version_match:
            return version_match.group(1)
        raise RuntimeError("Unable to find version string.")


def _supports_symlinks():
    """Check if the system supports symlinks (e.g. *nix) or not."""
    return getattr(os, "symlink", None) is not None


def build_jars():
    print("Start building ray jars")
    print("Generate maven deps for ray java")
    subprocess.check_call("bazel build //java:gen_maven_deps", shell=True)
    print("Build jars for ray java")
    subprocess.check_call("cd .. && mvn clean install -DskipTests", shell=True)
    print("Generate maven deps for ray streaming java")
    subprocess.check_call("bazel build //streaming/java:gen_maven_deps", shell=True)
    print("Build jars for ray streaming java")
    subprocess.check_call("cd ../../streaming/java && mvn clean install -DskipTests", shell=True)
    jars = {}
    for jar in os.listdir(ray_java_build_dir):
        jars[jar] = os.path.join(ray_java_build_dir, jar)
    for jar in os.listdir(ray_streaming_java_build_dir):
        jars[jar] = os.path.join(ray_streaming_java_build_dir, jar)

    if os.path.exists(jars_dir):
        shutil.rmtree(jars_dir, ignore_errors=True)
    os.mkdir(jars_dir)
    for jar, jar_path in jars.items():
        destination = os.path.join(jars_dir, jar)
        if _supports_symlinks():
            os.symlink(jar_path, destination)
        else:
            shutil.copy(jar_path, destination)
    print("Finished building ray jars")


build_jars()



setup(
    name="ray-java",
    packages=["ray_java"],
    version=find_version(),
    description="Package all ray java resources so that ray can create java process "
                "in python",
    long_description="Package all ray java resources so that python can create ray-related "
                     "java process",
    url="https://github.com/ray-project/ray",
    author="Ray Team",
    author_email="ray-dev@googlegroups.com",
    install_requires=['ray'],
    include_package_data=True,
    zip_safe=False,
    license="Apache 2.0")
