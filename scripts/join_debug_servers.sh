#!/bin/bash
#
# 既存のクラスタにディスクなしの frugalos ノードを join させるスクリプト。
# 主に setup_debug_cluster.sh と組み合わせて使うことを想定しています。
#
# USAGE:
#    API_BASE_URL=http://localhost:3333 CONTACT_SERVER=127.0.0.1:14200 SERVER_COUNT=3 ./scripts/join_debug_servers.sh 
#
#    API_BASE_URL: サーバーとデバイスを追加する時に利用する API の URL
#    CONTACT_SERVER: join する時に利用する RPC アドレス
#    JOIN_INTERVAL_SECONDS: join と join の間の実行間隔(秒指定)
#    PORT: join するサーバーの HTTP ポート指定。複数サーバー join する場合は 1 ずつインクレメントされる。
#    RPC_PORT: join するサーバーの RPC ポート指定。複数サーバー join する場合は 1 ずつインクレメントされる。
#    SERVER_COUNT: join するサーバー数(>= 1)
#    WORK_DIR: 作業用ディレクトリ。cluster.lusf や local.dat もこのディレクトリに作成される。

set -eux

MODE=debug
WORK_DIR=${WORK_DIR:-/tmp/frugalos_test/}
API_BASE_URL=${API_BASE_URL:-http://localhost:3100}
CONTACT_SERVER=${RPC_ADDR:-127.0.0.1:14278}
JOIN_INTERVAL_SECONDS=${JOIN_INTERVAL_SECONDS:-5}
# setup_debug_cluster.sh をデフォルト値で使った際ポートがかぶらないように開始ポートを選ぶ
RPC_PORT=${RPC_PORT:-14280}
PORT=${PORT:-3200}
SERVER_COUNT=${SERVER_COUNT:-1}
FRUGALOS_START_FLAGS=${FRUGALOS_START_FLAGS:- --sampling-rate 1.0}
FRUGALOS_CONFIG_FILE=`cd \`dirname $0\`; pwd`/frugalos.yml
FRUGALOS_CONFIG_FILE_PARAM="--config-file ${FRUGALOS_CONFIG_FILE}"

cd ${WORK_DIR} || exit 1

##
## Joins frugalos servers
##
for s in $(seq 1 ${SERVER_COUNT})
do
    server="srv$((${RPC_PORT} + ${s}))"
    http=$((${PORT} + ${s}))
    rpc=$((${RPC_PORT} + ${s}))
    tmux kill-window -t ${server} || echo "OK: ${server}"
    tmux new-window -d -n "${server}.0" -c ${WORK_DIR}
    # NOTE:
    # join に失敗するため先に leave する。
    # tmux コマンドの後に sleep を入れないとコマンド実行に失敗する。
    if [ -d ${server} ]; then
        tmux send-keys -t ${server}.0 "bin/frugalos leave --data-dir ${server} --contact-server ${CONTACT_SERVER}" C-m
        sleep 1
        tmux send-keys -t ${server}.0 "bin/frugalos stop --rpc-addr 127.0.0.1:${rpc}" C-m
        sleep 1
        rm -fr ${server}
    fi
    tmux send-keys -t ${server}.0 "bin/frugalos join --id ${server} --addr 127.0.0.1:${rpc} --data-dir ${server} --contact-server ${CONTACT_SERVER}" C-m
    sleep 1
    tmux send-keys -t ${server}.0 "bin/frugalos ${FRUGALOS_CONFIG_FILE_PARAM} start --data-dir ${server} ${FRUGALOS_START_FLAGS} --http-server-bind-addr 127.0.0.1:${http}" C-m
    # 連続で join と leave を実行すると unstable cluster で RPC に失敗しやすいので回避する
    sleep ${JOIN_INTERVAL_SECONDS}
done
