#! /bin/bash

set -eux

cargo build --all --release
cp target/release/frugalos docker/frugalos/
docker build -t frugalos docker/frugalos/
