#! /bin/bash

set -eux

source $(cd $(dirname $0); pwd)/common.sh

CLUSTER=three-nodes
# CLUSTER=nine-nodes

#
# Cleanups previous garbages
#
docker-compose -f it/clusters/${CLUSTER}.yml down
sudo rm -rf /tmp/frugalos_it/

#
# Setups cluster
#
docker-compose -f it/clusters/${CLUSTER}.yml up -d
mkdir -p ${WORK_DIR}
sudo chmod 777 ${WORK_DIR}
sleep 1
curl -f http://frugalos01/v1/servers | tee $WORK_DIR/servers.json
SERVERS=`jq 'map(.id) | .[]' /tmp/frugalos_it/servers.json | sed -e 's/"//g'`

#
# Setups devices
#
it/scripts/put_devices.sh 1 $SERVERS

#
# Setups buckets
#
it/scripts/put_buckets.sh 1 2
curl http://frugalos01/v1/buckets

#
# Puts objects
#
it/scripts/gen_put_requests.sh frugalos01 live_archive_chunk 1 1000 $WORK_DIR/req.json
sleep 10
hb run -i $WORK_DIR/req.json | hb summary
sleep 10
hb run -i $WORK_DIR/req.json | hb summary

#
# GET => DELETE => GET
#
it/scripts/http_requests.sh GET 200 $WORK_DIR/req.json $WORK_DIR/res.json
it/scripts/http_requests.sh DELETE 200 $WORK_DIR/req.json $WORK_DIR/res.json
it/scripts/http_requests.sh GET 404 $WORK_DIR/req.json $WORK_DIR/res.json

#
# Cleanups cluster
#
docker-compose -f it/clusters/${CLUSTER}.yml down
