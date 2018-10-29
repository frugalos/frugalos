#!/bin/bash
#
# Publish the frugalos binary of the specified version to Docker Hub.
#
# Usage: publish.sh $FRUGALOS_VERSION

set -eux

FRUGALOS_VERSION=$1

docker build --build-arg FRUGALOS_VERSION=$FRUGALOS_VERSION -t frugalos/frugalos:${FRUGALOS_VERSION} .
docker push frugalos/frugalos:${FRUGALOS_VERSION}
