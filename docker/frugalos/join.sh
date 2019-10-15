#!/bin/bash

set -eux

if ! [ -f $FRUGALOS_DATA_DIR/cluster.lusf ]
then
    sleep 1
    frugalos join --addr `hostname -i`:8080 --contact-server 172.18.0.21:8080
fi
frugalos --config-file $FRUGALOS_CONFIG_DIR/frugalos.yml start \
         --http-server-bind-addr 0.0.0.0:80
