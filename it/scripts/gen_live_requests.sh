#! /bin/bash

set -eux

API_HOST=frugalos01
ID_BEGIN=$1
ID_END=$2

##
## LIVEを登録するためのデータを生成する。
## 以下の形式のデータを生成する。
## [
##   { "method": "PUT", "content": "live1", "url": "http://frugalos01/v1/buckets/chunk/objects/live1" }
## ]
##

echo $(jo -a $(seq 1 ${ID_BEGIN} ${ID_END} | xargs -I {} jo method=PUT content="live{}" url=http://${API_HOST}/v1/buckets/chunk/objects/live{}))

