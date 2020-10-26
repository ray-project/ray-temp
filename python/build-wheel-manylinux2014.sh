#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

cat << EOF > "/usr/bin/nproc"
#!/bin/bash
echo 10
EOF
chmod +x /usr/bin/nproc

NODE_VERSION="14"
PYTHONS=("cp36-cp36m"
         "cp37-cp37m"
         "cp38-cp38")

# The minimum supported numpy version is 1.14, see
# https://issues.apache.org/jira/browse/ARROW-3141
NUMPY_VERSIONS=("1.14.5"
                "1.14.5"
                "1.14.5")

yum -y install unzip zip sudo
yum -y install java-1.8.0-openjdk java-1.8.0-openjdk-devel xz

/ray/ci/travis/install-bazel.sh
# Put bazel into the PATH if building Bazel from source
# export PATH=/root/bazel-3.2.0/output:$PATH:/root/bin

echo "build --config=manylinux2014" >> /root/.bazelrc


# Install and use the latest version of Node.js in order to build the dashboard.
set +x
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.34.0/install.sh | bash
source "$HOME"/.nvm/nvm.sh
nvm install "$NODE_VERSION"
nvm use "$NODE_VERSION"

# Build the dashboard so its static assets can be included in the wheel.
# TODO(mfitton): switch this back when deleting old dashboard code.
pushd python/ray/new_dashboard/client
  npm ci
  npm run build
popd
set -x

mkdir -p .whl
for ((i=0; i<${#PYTHONS[@]}; ++i)); do
  PYTHON=${PYTHONS[i]}
  NUMPY_VERSION=${NUMPY_VERSIONS[i]}

  # The -f flag is passed twice to also run git clean in the arrow subdirectory.
  # The -d flag removes directories. The -x flag ignores the .gitignore file,
  # and the -e flag ensures that we don't remove the .whl directory and the
  # dashboard directory.
  git clean -f -f -x -d -e .whl -e python/ray/new_dashboard/client -e dashboard/client

  export BAZEL_LINKLIBS="-l%:libstdc++.a"

  pushd python
    # Fix the numpy version because this will be the oldest numpy version we can
    # support.
    /opt/python/"${PYTHON}"/bin/pip install -q numpy=="${NUMPY_VERSION}" cython==0.29.15
    # Set the commit SHA in __init__.py.
    if [ -n "$TRAVIS_COMMIT" ]; then
      sed -i.bak "s/{{RAY_COMMIT_SHA}}/$TRAVIS_COMMIT/g" ray/__init__.py && rm ray/__init__.py.bak
    else
      echo "TRAVIS_COMMIT variable not set - required to populated ray.__commit__."
      exit 1
    fi

    PATH=/opt/python/${PYTHON}/bin:/root/bazel-3.2.0/output:$PATH \
    /opt/python/"${PYTHON}"/bin/python setup.py bdist_wheel
    # In the future, run auditwheel here.
    mv dist/*.whl ../.whl/
  popd
done

# Rename the wheels so that they can be uploaded to PyPI. TODO(rkn): This is a
# hack, we should use auditwheel instead.
for path in .whl/*.whl; do
  if [ -f "${path}" ]; then
    mv "${path}" "${path//linux/manylinux2014}"
  fi
done
