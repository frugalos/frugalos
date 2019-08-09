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
it/scripts/http_requests.sh GET 200 $WORK_DIR/req.json $WORK_DIR/res.json || echo "OK"

# Waits for repairing to complete, at most for 120 seconds.
SUCC=0
for i in `seq 1 40`
do
  # Constantly send repair requests, because frugalos shortly after booting can fail to serve these requests.
  docker exec clusters_frugalos01_1 \
    frugalos set-repair-config --rpc-addr 172.18.0.22:8080 --repair-idleness-threshold 0.5 >/dev/null # sends rpc
  SUCC=`curl frugalos02/metrics 2>/dev/null | grep repairs_success_total | awk 'BEGIN { a = 0; } { a+= $2; } END { print a; }'`
  echo "i=${i} succ=${SUCC}"
  if [ ${SUCC} -eq 1000 ]
  then
    break
  fi
  sleep 3 # Waits for repairing
done
if [ ${SUCC} -ne 1000 ]
then
  echo "Error! Repairing didn't finish in 120 seconds" 1>&2
  exit 1
fi

#
# Kills the third server
#
docker-compose -f $CLUSTER kill frugalos03

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
