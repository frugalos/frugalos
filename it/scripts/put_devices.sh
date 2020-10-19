#! /bin/bash

set -eux

API_HOST=frugalos01
DEVICE_CAPACITY=$((100 * 1024 * 1024))
DISK_DIR_ROOT=/var/lib/frugalos

DEVICES_PER_SERVER=$1
shift
SERVERS=$@

##
## 物理デバイス登録
##
for server in $SERVERS
do
    for device in `seq 1 $DEVICES_PER_SERVER`
    do
        device_id="${server}_${device}"
        JSON=$(cat <<EOF
{"file": {
  "id": "${device_id}",
  "server": "${server}",
  "capacity": ${DEVICE_CAPACITY},
  "filepath": "${DISK_DIR_ROOT}/device_${device}.lusf"
}}
EOF
            )
        curl -f -XPUT -d "$JSON" http://$API_HOST/v1/devices/${device_id}
    done
done

##
## 仮想デバイス登録
##
for server in $SERVERS
do
    CHILDREN=""
    for device in `seq 1 $DEVICES_PER_SERVER`
    do
        device_id="${server}_${device}"
        if [ "${CHILDREN}" != "" ]
        then
            CHILDREN="${CHILDREN},"
        fi
        CHILDREN="${CHILDREN} \"${device_id}\""
    done
    JSON=$(cat <<EOF
{"virtual": {
  "id": "${server}",
  "children": [${CHILDREN}]
}}
EOF
            )
    curl -f -XPUT -d "$JSON" http://$API_HOST/v1/devices/${server}
done

CHILDREN=`echo $SERVERS | sed -e 's/ /","/g'`
JSON=$(cat <<EOF
{"virtual": {
  "id": "rack",
  "children": ["${CHILDREN}"]
}}
EOF
    )
curl -f -XPUT -d "$JSON" http://$API_HOST/v1/devices/store01
