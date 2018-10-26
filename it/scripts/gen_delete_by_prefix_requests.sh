#! /bin/bash

set -eux

API_HOST=frugalos01
BUCKET=$1
OBJECT_PREFIX=$2

##
## 接頭辞削除用のデータを生成する。
## 以下の形式でデータが生成される。
## [
##   { "method": "DELETE", "http://frugalos01/v1/buckets/chunk/object_prefixes/xxx" }
## ]
##

echo $(jo -a $(seq 1 1 | jo method=DELETE content="" url=http://${API_HOST}/v1/buckets/${BUCKET}/object_prefixes/${OBJECT_PREFIX}))

