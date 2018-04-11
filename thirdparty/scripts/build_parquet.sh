#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

unamestr="$(uname)"
TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)/../
PARQUET_HOME=$TP_DIR/pkg/arrow/cpp/build/cpp-install
OPENSSL_DIR=/usr/local/opt/openssl
BISON_DIR=/usr/local/opt/bison/bin

if [ ! -d $TP_DIR/build/parquet-cpp ]; then
  git clone https://github.com/apache/parquet-cpp.git "$TP_DIR/build/parquet-cpp"
  pushd $TP_DIR/build/parquet-cpp
  git fetch origin master
  git checkout 4e7ef12dccb250074370376dc31a4963e1010447

  # The below cherry-pick is to fix a segfault when linking boost statically in
  # parquet. See https://issues.apache.org/jira/browse/ARROW-2247.
  git remote add majetideepak https://github.com/majetideepak/parquet-cpp
  git fetch majetideepak
  git cherry-pick --no-commit b23d3b33de25ece8544f206e2d2c9c1d41aaddc2

  if [ "$unamestr" == "Darwin" ]; then
    OPENSSL_ROOT_DIR=$OPENSSL_DIR \
    PATH="$BISON_DIR:$PATH" \
    BOOST_ROOT=$TP_DIR/pkg/boost \
    ARROW_HOME=$TP_DIR/pkg/arrow/cpp/build/cpp-install \
    cmake -DCMAKE_BUILD_TYPE=Release \
          -DCMAKE_INSTALL_PREFIX=$PARQUET_HOME \
          -DPARQUET_BUILD_BENCHMARKS=off \
          -DPARQUET_BUILD_EXECUTABLES=off \
          -DPARQUET_BUILD_TESTS=off \
          .

    OPENSSL_ROOT_DIR=$OPENSSL_DIR \
    PATH="$BISON_DIR:$PATH" \
    make -j4

    OPENSSL_ROOT_DIR=$OPENSSL_DIR \
    PATH="$BISON_DIR:$PATH" \
    make install
  else
    BOOST_ROOT=$TP_DIR/pkg/boost \
    ARROW_HOME=$TP_DIR/pkg/arrow/cpp/build/cpp-install \
    cmake -DCMAKE_BUILD_TYPE=Release \
          -DCMAKE_INSTALL_PREFIX=$PARQUET_HOME \
          -DPARQUET_BUILD_BENCHMARKS=off \
          -DPARQUET_BUILD_EXECUTABLES=off \
          -DPARQUET_BUILD_TESTS=off \
          .

    PARQUET_HOME=$TP_DIR/pkg/arrow/cpp/build/cpp-install \
    BOOST_ROOT=$TP_DIR/pkg/boost \
    make -j4
    make install
  fi

  popd
fi
