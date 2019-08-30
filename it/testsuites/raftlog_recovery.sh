#! /bin/bash
# 1 プロセスでクラスタを組んでいる場合に、適当に propose した後に raft のログを
# 削除(かつ、成功)すればディスク上には PUT したデータが残っているがデータの位置
# が分からなくなり GET ができなくなるという特性を妥当性の検証として利用。

set -eux

source $(cd $(dirname $0); pwd)/common.sh

CLUSTER=single-node
EXPECTED_TOTAL=100
SLEEP_SECONDS=10

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
it/scripts/gen_put_requests.sh frugalos01 live_archive_chunk 1 $EXPECTED_TOTAL $WORK_DIR/req.json
sleep $SLEEP_SECONDS
hb run -i $WORK_DIR/req.json | hb summary

#
# ログを削除 => GET が 404 になる
#
docker-compose -f it/clusters/single-node.yml exec frugalos01 sh -c 'touch /var/lib/frugalos/recovery.yml'
docker-compose -f it/clusters/${CLUSTER}.yml restart frugalos01
sleep $SLEEP_SECONDS
it/scripts/http_requests.sh GET 404 $WORK_DIR/req.json $WORK_DIR/res.json $EXPECTED_TOTAL

#
# raft のログを削除した後に PUT => GET ができることを確認
#
hb run -i $WORK_DIR/req.json | hb summary
it/scripts/http_requests.sh GET 200 $WORK_DIR/req.json $WORK_DIR/res.json $EXPECTED_TOTAL

#
# Cleanups cluster
#
docker-compose -f it/clusters/${CLUSTER}.yml down
