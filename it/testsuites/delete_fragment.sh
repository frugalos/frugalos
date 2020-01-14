#! /bin/bash

set -eux

source $(cd $(dirname $0); pwd)/common.sh

CLUSTER=three-nodes
HOST=frugalos01
BUCKET_ID=live_archive_chunk

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
SERVERS=`jq 'map(.id) | .[]' /tmp/frugalos_it/servers.json | sed -e 's/"//g'`

#
# Setups devices
#
it/scripts/put_devices.sh 1 ${SERVERS}

#
# Setups buckets
#
it/scripts/put_buckets.sh 1 2
curl http://frugalos01/v1/buckets

function curl_assert() {
    local ARGS=$1
    local EXPECT_STATUS=$2
    [ $EXPECT_STATUS -eq `curl -o /dev/null -s -w "%{http_code}\n" $ARGS` ]
}

# put
sleep 20
curl_assert "-X PUT -d 'foo' http://${HOST}/v1/buckets/${BUCKET_ID}/objects/foo" "201"

# head/get
curl_assert "-I -s http://${HOST}/v1/buckets/${BUCKET_ID}/objects/foo" "200"
curl_assert "-s http://${HOST}/v1/buckets/${BUCKET_ID}/objects/foo" "200"

# delete_fragment
curl_assert "-X DELETE http://${HOST}/v1/buckets/${BUCKET_ID}/objects/foo/fragments/0" "200"
curl_assert "-X DELETE http://${HOST}/v1/buckets/${BUCKET_ID}/objects/foo/fragments/1" "200"

# head/get
curl_assert "-I -s http://${HOST}/v1/buckets/${BUCKET_ID}/objects/foo" "200"
curl_assert "-s http://${HOST}/v1/buckets/${BUCKET_ID}/objects/foo" "500"

docker-compose -f it/clusters/${CLUSTER}.yml down
