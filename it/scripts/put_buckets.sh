#! /bin/bash

set -eux

API_HOST=frugalos01
TOLERABLE_FAULTS=$1
DATA_FRAGMENTS=$2

##
## バケツ登録
##
JSON=$(cat <<EOF
{"metadata": {
  "id": "live_archive_metadata",
  "device": "rack",
  "tolerable_faults": $TOLERABLE_FAULTS
}}
EOF
)
curl -f -XPUT -d "$JSON" http://$API_HOST/v1/buckets/live_archive_metadata

JSON=$(cat <<EOF
{"dispersed": {
  "id": "live_archive_chunk",
  "device": "rack",
  "tolerable_faults": $TOLERABLE_FAULTS,
  "data_fragment_count": $DATA_FRAGMENTS
}}
EOF
)
curl -f -XPUT -d "$JSON" http://$API_HOST/v1/buckets/live_archive_chunk

JSON=$(cat <<EOF
{"dispersed": {
  "id": "chunk",
  "device": "rack",
  "tolerable_faults": $TOLERABLE_FAULTS,
  "data_fragment_count": $DATA_FRAGMENTS
}}
EOF
)
curl -f -XPUT -d "$JSON" http://$API_HOST/v1/buckets/chunk

JSON=$(cat <<EOF
{"replicated": {
  "id": "replicated",
  "device": "rack",
  "tolerable_faults": $TOLERABLE_FAULTS
}}
EOF
)
curl -f -XPUT -d "$JSON" http://$API_HOST/v1/buckets/replicated
