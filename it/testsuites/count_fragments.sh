#! /bin/bash

set -eux

source $(cd $(dirname $0); pwd)/common.sh

CLUSTER=three-nodes

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
HOST=frugalos03
HOST_IP=`getent ahosts $HOST | head -1 | awk '{print $1}'`
curl -f http://$HOST_IP/v1/servers | tee $WORK_DIR/servers.json
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
sleep 5
curl -s -X PUT -d "test" "http://$HOST_IP/v1/buckets/live_archive_chunk/objects/test1"
sleep 5
curl -s -X PUT -d "test" "http://$HOST_IP/v1/buckets/live_archive_chunk/objects/test1"
sleep 5

#
# 全台揃っている状況での GET
#
curl -s -I "http://$HOST_IP/v1/buckets/live_archive_chunk/objects/test1/fragments" | fgrep "FrugalOS-Fragments-Corrupted: false"
curl -s -I "http://$HOST_IP/v1/buckets/live_archive_chunk/objects/test1/fragments" | fgrep "FrugalOS-Fragments-Found-Total: 3"
curl -s -I "http://$HOST_IP/v1/buckets/live_archive_chunk/objects/test1/fragments" | fgrep "FrugalOS-Fragments-Lost-Total: 0"

#
# ノードが 1 台停止中での GET
#
docker-compose -f it/clusters/${CLUSTER}.yml stop frugalos02
# ハートビート間隔より長い時間待つ必要がある
sleep 10
curl -s -I "http://$HOST_IP/v1/buckets/live_archive_chunk/objects/test1/fragments" | fgrep "FrugalOS-Fragments-Corrupted: false"
curl -s -I "http://$HOST_IP/v1/buckets/live_archive_chunk/objects/test1/fragments" | fgrep "FrugalOS-Fragments-Found-Total: 2"
curl -s -I "http://$HOST_IP/v1/buckets/live_archive_chunk/objects/test1/fragments" | fgrep "FrugalOS-Fragments-Lost-Total: 1"

#
# Cleanups cluster
#
sleep 5
docker-compose -f it/clusters/${CLUSTER}.yml down
