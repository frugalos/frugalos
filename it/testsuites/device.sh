#! /bin/bash

set -eux

source $(
    cd $(dirname $0)
    pwd
)/common.sh

set +x

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
SERVERS=$(jq 'map(.id) | .[]' /tmp/frugalos_it/servers.json | sed -e 's/"//g')

#
# Setups devices
#
it/scripts/put_devices.sh 1 $SERVERS

#
# Setups buckets
#
it/scripts/put_buckets.sh 1 2
curl -s http://frugalos01/v1/buckets | jq '.[] | .id'

#
# Start
#

#
# Check if state is started
#
function check_state() {
    curl -fs http://frugalos01/v1/devices/frugalos01_1/state | jq '.state'
}
STATE=$(check_state)

if [ ${STATE} != '"started"' ]; then
    echo "STATE should be started"
    exit 1
fi

#
# Stop the device
#
curl -fs http://frugalos01/v1/devices/frugalos01_1/state -XPUT -d '{"state":"stopped"}' >/dev/null

STATE=$(check_state)

if [ ${STATE} != '"stopped"' ]; then
    echo "STATE should be stopped"
    exit 1
fi

#
# Even though frugalos01_1 is stopped, PUT requests should not fail.
#
it/scripts/gen_put_requests.sh frugalos01 replicated 1 1000 $WORK_DIR/req.json
EXIT_STATUS=1
#
# Try to put until all 1000 requests succeed.
#
echo "Trying to put 1000 objects..." 1>&2
for i in $(seq 1 10); do
    STATUS=$(hb run -i $WORK_DIR/req.json | hb summary | jq '.status')
    echo ${i} status=${STATUS} 1>&2
    if [ $(echo ${STATUS} | jq '."200"') = 1000 ]; then
        EXIT_STATUS=0
        break
    fi
    sleep 5
done

if [ ${EXIT_STATUS} -ne 0 ]; then
    echo "Failed to put 1000 objects." 1>&2
    exit ${EXIT_STATUS}
fi
