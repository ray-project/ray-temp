#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

if [ ! -f redis-3.2.3/src/redis-server ]; then
  wget http://download.redis.io/releases/redis-3.2.3.tar.gz
  tar xvfz redis-3.2.3.tar.gz
  cd redis-3.2.3
  make
fi
