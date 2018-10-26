#! /bin/bash

set -eu

HOST=$1
HOST_IP=`getent ahosts $HOST | head -1 | awk '{print $1}'`
BUCKET=$2
ID_START=$3
ID_END=$4
OUTPUT_FILE=$5

jo -a `for n in $(seq $ID_START $ID_END); do jo method=PUT content=1024 url=http://$HOST_IP/v1/buckets/$BUCKET/objects/$n; done` > $OUTPUT_FILE
