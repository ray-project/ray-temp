#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e
# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

# Skip compiling the binaries if `--skip-compile` is passed in.
if [[ "$@" != *--skip-compile* ]]; then
    echo "Compiling binaries."
    $ROOT_DIR/../build.sh -l java
fi

pushd $ROOT_DIR/../java
echo "Compiling Java code."
mvn clean install -Dmaven.test.skip

echo "Checking code format."
mvn checkstyle:check

echo "Running tests under cluster mode."
mvn test

echo "Running tests under single-process mode."
ENABLE_MULTI_LANGUAGE_TESTS=0 mvn test -Dray.run-mode=SINGLE_PROCESS

set +x
set +e

popd
