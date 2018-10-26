#!/bin/bash

set -eux

METHOD=$1
EXPECTED_STATUS=$2
REQUEST_FILE=$3
RESPONSE_FILE=$4
EXPECTED_TOTAL=${5:-1000} # デフォルト 1000

jq "map({method: \"${METHOD}\", url: .url})" $REQUEST_FILE | hb run -o $RESPONSE_FILE
hb summary -i $RESPONSE_FILE
[ `hb summary -i $RESPONSE_FILE | jq ".status.\"${EXPECTED_STATUS}\""` -eq "${EXPECTED_TOTAL}" ]
