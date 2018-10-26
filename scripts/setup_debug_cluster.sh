#!/bin/bash

set -eux

WIN=frugalos_test
WORK_DIR=/tmp/frugalos_test/
MODE=debug
RPC_PORT=14278
PORT=3100

##
## Initialize working directory
##
rm -rf $WORK_DIR
mkdir -p $WORK_DIR
mkdir $WORK_DIR/bin
cp target/${MODE}/frugalos $WORK_DIR/bin/

##
## Creates window and panes
##
tmux kill-window -t $WIN || echo "OK"
tmux new-window -n $WIN -c $WORK_DIR

##
## Starts frugalos cluster
##
tmux send-keys -t $WIN.0 "bin/frugalos create --id srv0 --addr 127.0.0.1:${RPC_PORT} --data-dir srv0" C-m
tmux send-keys -t $WIN.0 "bin/frugalos start --data-dir srv0 --sampling-rate 1.0 --http-server-bind-addr 127.0.0.1:${PORT}" C-m
sleep 6

##
## Put devices
##
DEVICES=""
for s in `echo srv{0..0}`
do
    for d in `echo {0..0}`
    do
        echo "# server=${s}, device=dev${d}"

        JSON=$(cat <<EOF
{"file": {
  "id": "${s}_dev${d}",
  "server": "${s}",
  "filepath": "$WORK_DIR/$s/$d/dev.lusf"
}}
EOF
            )
        curl -XPUT -d "$JSON" http://localhost:${PORT}/v1/devices/${s}_dev${d}
        DEVICES="${DEVICES} \"${s}_dev${d}\""
    done
done

DEVICES=`echo $DEVICES | sed -e 's/ /,/g'`

JSON=$(cat <<EOF
{"virtual": {
  "id": "rack0",
  "children": [${DEVICES}]
}}
EOF
    )
curl -XPUT -d "$JSON" http://localhost:${PORT}/v1/devices/rack0


##
## Put buckets
##
JSON=$(cat <<EOF
{"metadata": {
  "id": "live_archive_metadata",
  "device": "rack0",
  "tolerable_faults": 4
}}
EOF
)
curl -XPUT -d "$JSON" http://localhost:${PORT}/v1/buckets/live_archive_metadata

JSON=$(cat <<EOF
{"dispersed": {
  "id": "live_archive_chunk",
  "device": "rack0",
  "tolerable_faults": 4,
  "data_fragment_count": 8
}}
EOF
)
curl -XPUT -d "$JSON" http://localhost:${PORT}/v1/buckets/live_archive_chunk
