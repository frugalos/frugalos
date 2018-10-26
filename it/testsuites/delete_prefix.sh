#! /bin/bash

set -eux

source $(cd $(dirname $0); pwd)/common.sh

CLUSTER=three-nodes
TIMEOUT=3
HOST=frugalos01

#
# Cleanups previous garbages
#
docker-compose -f it/clusters/${CLUSTER}.yml down
sudo rm -rf ${WORK_DIR}

#
# Setups cluster
#
docker-compose -f it/clusters/${CLUSTER}.yml up -d
mkdir -p ${WORK_DIR}
sudo chmod 777 ${WORK_DIR}
sleep 1
curl -f http://${HOST}/v1/servers | tee ${WORK_DIR}/servers.json
SERVERS=`jq 'map(.id) | .[]' ${WORK_DIR}/servers.json | sed -e 's/"//g'`

#
# Setups devices
#
it/scripts/put_devices.sh 1 ${SERVERS}

#
# Setups buckets
#
it/scripts/put_buckets.sh 1 2
curl http://${HOST}/v1/buckets

#
# Setups requests
#
it/scripts/gen_delete_by_prefix_requests.sh chunk live > ${WORK_DIR}/delete_chunks_with_live_prefix.json
it/scripts/gen_live_requests.sh 1 5 > ${WORK_DIR}/put_lives.json
it/scripts/gen_video_requests.sh 1 5 > ${WORK_DIR}/put_videos.json

#
# Puts objects
#
sleep 20 # 500 が返ってしまうケースがあるので長めに sleep して回避する
hb run -i ${WORK_DIR}/put_lives.json | hb summary
sleep ${TIMEOUT}
hb run -i ${WORK_DIR}/put_videos.json | hb summary
sleep ${TIMEOUT}

#
# DELETE(by prefix)
# live だけ消えていることを確認する。
#
it/scripts/http_requests.sh DELETE 200 ${WORK_DIR}/delete_chunks_with_live_prefix.json ${WORK_DIR}/res.json 1
sleep ${TIMEOUT}
it/scripts/http_requests.sh GET 404 ${WORK_DIR}/put_lives.json ${WORK_DIR}/res.json 5
sleep ${TIMEOUT}
it/scripts/http_requests.sh GET 200 ${WORK_DIR}/put_videos.json ${WORK_DIR}/res.json 5
sleep ${TIMEOUT}

#
# Cleanups cluster
#
docker-compose -f it/clusters/${CLUSTER}.yml down
