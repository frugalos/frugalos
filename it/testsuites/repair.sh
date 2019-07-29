#! /bin/bash
#
# 三ノード中の一つがダウンしていても正常に操作が行えるかどうかの確認。
# 二番目のノード(frugalos02)がダウンしているようにする。

set -eux

WORK_DIR=/tmp/frugalos_it
CLUSTER=it/clusters/three-nodes.yml
# CLUSTER=nine-nodes

#
# Cleanups previous garbages
#
docker-compose -f $CLUSTER down
sudo rm -rf /tmp/frugalos_it/

#
# Setups cluster
#
docker-compose -f $CLUSTER up -d
sudo chmod 777 /tmp/frugalos_it/
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
# Kills the second server
#
docker-compose -f $CLUSTER kill frugalos02

#
# Puts objects
#
it/scripts/gen_put_requests.sh frugalos01 live_archive_chunk 1 1000 $WORK_DIR/req.json
sleep 30
hb run -i $WORK_DIR/req.json | hb summary
hb run -i $WORK_DIR/req.json | hb summary

#
# GET
#
it/scripts/http_requests.sh GET 200 $WORK_DIR/req.json $WORK_DIR/res.json

#
# Restarts the second server
#
docker-compose -f $CLUSTER start frugalos02
sleep 50  # Waits for starting
docker exec clusters_frugalos01_1 frugalos set-repair-idleness-threshold --rpc-addr 172.18.0.22:8080 --repair-idleness-threshold 0 # sends rpc
it/scripts/http_requests.sh GET 200 $WORK_DIR/req.json $WORK_DIR/res.json || echo "OK"
sleep 60  # Waits for repairing

#
# Kills the third server
#
docker-compose -f $CLUSTER kill frugalos03
sleep 30

#
# GET => DELETE => GET
#
it/scripts/http_requests.sh GET 200 $WORK_DIR/req.json $WORK_DIR/res.json || echo "OK"
it/scripts/http_requests.sh GET 200 $WORK_DIR/req.json $WORK_DIR/res.json
it/scripts/http_requests.sh DELETE 200 $WORK_DIR/req.json $WORK_DIR/res.json
it/scripts/http_requests.sh GET 404 $WORK_DIR/req.json $WORK_DIR/res.json

#
# Cleanups cluster
#
docker-compose -f $CLUSTER down
