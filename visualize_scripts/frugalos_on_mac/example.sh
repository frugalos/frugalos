#!/bin/bash

set -eux

DATADIR='frugalos_temporary_data_dir'

rm -rf 'frugalos_temporary_data_dir'

EXAMPLE='example'
FRUGALOS="../../target/debug/frugalos"

$FRUGALOS create --id $EXAMPLE --data-dir $DATADIR/

tmux split-window "RAFT_IO_MAX_LUMP_DATA_SIZE=512 FRUGALOS_SNAPSHOT_THRESHOLD=10 ${FRUGALOS} -l debug start --data-dir ${DATADIR}; read"

sleep 5

DEVICE_JSON='{"file": {"id": "file0", "server": "'$EXAMPLE'", "filepath": "'$DATADIR'/file0.lusf"}}'
curl -XPUT -d "$DEVICE_JSON" http://localhost:3000/v1/devices/file0
echo ""

sleep 3

BUCKET_JSON=$(cat <<EOF
{
  "dispersed": {
    "id": "bucket0",
    "device": "file0",
    "tolerable_faults": 4,
    "data_fragment_count": 6
  }
}
EOF
)
curl -XPUT -d "$BUCKET_JSON" http://localhost:3000/v1/buckets/bucket0
echo ""

sleep 3

curl -XPUT -d 'your_object_data' http://localhost:3000/v1/buckets/bucket0/objects/your_object_id

sleep 3

curl http://localhost:3000/v1/buckets/bucket0/objects/your_object_id
