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

it/scripts/gen_put_requests.sh $HOST live_archive_chunk 1 1000 $WORK_DIR/req.json
sleep 5
hb run -i $WORK_DIR/req.json | hb summary
sleep 5
hb run -i $WORK_DIR/req.json | hb summary
sleep 30

#
# GET/HEAD
#
declare -a query_params=("?consistency=stale" "?consistency=quorum" "?consistency=subset&subset=1" "?consistency=subset&subset=2" "?consistency=consistent")
for q in ${query_params[@]}
do
    # FIXME: http_requests.sh より汎用的に使える HTTP リクエスト実行ツールが必要
    hb run -i <(jo -a $(jo method=GET url=http://$HOST_IP/v1/buckets/live_archive_chunk/segments/0/objects$q)) -o $WORK_DIR/res.json
    [ `hb summary -i $WORK_DIR/res.json | jq ".status.\"200\""` -eq "1" ]
    hb run -i <(jo -a $(jo method=GET url=http://$HOST_IP/v1/buckets/live_archive_chunk/stats$q)) -o $WORK_DIR/res.json
    [ `hb summary -i $WORK_DIR/res.json | jq ".status.\"200\""` -eq "1" ]
    QUERY_PARAMS="$q" it/scripts/http_requests.sh GET 200 $WORK_DIR/req.json $WORK_DIR/res.json 1000
    QUERY_PARAMS="$q" it/scripts/http_requests.sh HEAD 200 $WORK_DIR/req.json $WORK_DIR/res.json 1000
done

#
# ノードが1台停止中での GET/HEAD
#
docker-compose -f it/clusters/${CLUSTER}.yml stop frugalos02
# ハートビート間隔より長い時間待つ必要がある
sleep 10
for q in ${query_params[@]}
do
    hb run -i <(jo -a $(jo method=GET url=http://$HOST_IP/v1/buckets/live_archive_chunk/segments/0/objects$q)) -o $WORK_DIR/res.json
    [ `hb summary -i $WORK_DIR/res.json | jq ".status.\"200\""` -eq "1" ]
    hb run -i <(jo -a $(jo method=GET url=http://$HOST_IP/v1/buckets/live_archive_chunk/stats$q)) -o $WORK_DIR/res.json
    [ `hb summary -i $WORK_DIR/res.json | jq ".status.\"200\""` -eq "1" ]
    QUERY_PARAMS="$q" it/scripts/http_requests.sh GET 200 $WORK_DIR/req.json $WORK_DIR/res.json 1000
    QUERY_PARAMS="$q" it/scripts/http_requests.sh HEAD 200 $WORK_DIR/req.json $WORK_DIR/res.json 1000
done

#
# Cleanups cluster
#
sleep 5
docker-compose -f it/clusters/${CLUSTER}.yml down
