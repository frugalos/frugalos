#!/bin/bash

set -eux

if ! [ -f $FRUGALOS_DATA_DIR/cluster.lusf ]
then
    frugalos create --addr 172.18.0.21:8080
fi
frugalos --config-file $FRUGALOS_CONFIG_DIR/frugalos.yml start \
         --http-server-bind-addr 0.0.0.0:80
