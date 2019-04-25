it (integration test)
===

結合テスト

準備
----

```
# in: /etc/hosts
#
# 利便性を考慮して、以下のエントリを追加しておく
172.18.0.21 frugalos01
```

```console
// 前回のデータを完全に削除したい場合
$ rm -rf /tmp/frugalos_it/
```

### frugalosバイナリの作成
以下の`イメージのビルド`項ではfrugalosバイナリが必要なので以下の何れかで作成しておく:

#### 普通にビルドする
```console
frugalos$ cargo build --release
frugalos$ ls target/release/frugalos
target/release/frugalos
```
注意: Linux 64bit環境で作成してでない場合、dockerイメージに送り込んでも使えない可能性がある。
その場合は次の方法でビルドすれば良い。

#### `docker/frugalos-build`を用いる
`docker/frugalos-build`下の`Dockerfile`を用いてfrugalosバイナリを作る。
```console
frugalos/docker/frugalos-build$ docker build -t frugalos-build .
frugalos/docker/frugalos-build$ tmp_id=$(docker create frugalos-build)
frugalos/docker/frugalos-build$ docker cp ${tmp_id}:frugalos/target/release/frugalos frugalos
frugalos/docker/frugalos-build$ docker rm -v $tmp_id # コンテナの削除
frugalos/docker/frugalos-build$ ls
Dockerfile		frugalos
```

イメージのビルド
----------------

`docker/frugalos`を用いてイメージをビルドする。

```console
frugalos/docker$ ls
frugalos		frugalos-build		frugalos-release	hub
frugalos/docker$ cp ../target/release/frugalos frugalos
# `frugalos-build`を使った場合はそこから取ってくる
frugalos/docker$ cd frugalos
frugalos/docker/frugalos$ docker build -t frugalos .
frugalos/docker/frugalos$ docker run -it --rm frugalos frugalos
Usage: USAGE:
    frugalos [OPTIONS] [SUBCOMMAND]
```

クラスタの起動
---------------

```console
frugalos/it$ ls
README.md	clusters	frugalos.env	scripts		testsuites
frugalos/it$ docker-compose -f clusters/three-nodes.yml up

// 別シェルで実行
$ curl http://frugalos01/v1/servers
[
  {
    "id": "frugalos01"
  },
  {
    "id": "frugalos02"
  },
  {
    "id": "frugalos03"
  }
}
```

デバイスおよびバケツの登録
--------------------------

```console
// 三台のサーバにデバイスを登録.
// 各サーバ毎のデバイス数は2.
frugalos/it$ scripts/put_devices.sh 2 frugalos01 frugalos02 frugalos03

// parity_fragments=1, data_fragments=2, でバケツを登録
frugalos/it$ scripts/put_buckets.sh 1 2

frugalos/it$ curl http://frugalos01/v1/buckets
[
  {
    "id": "live_archive_chunk",
    "type": "dispersed",
    "device": "rack"
  },
  {
    "id": "live_archive_metadata",
    "type": "metadata",
    "device": "rack"
  }
}
```

テストスイートを用いたテスト
--------------------------

テストスイートを用いるには以下をインストールしパスを通しておくこと:
* `jq` https://github.com/stedolan/jq
* `jo` https://github.com/jpmens/jo
* `hb` https://github.com/sile/hb (`cargo install hb`でインストールできる)

## 例
`basic.sh`を用いて、クラスタを作った上で、オブジェクトのPUT, GET, DELETEが行えるかテストする。

```console
frugalos$ ./it/testsuites/basic.sh
+++ dirname ./it/testsuites/basic.sh
++ cd ./it/testsuites
++ pwd
+ source /home/yuezato/frugalos/it/testsuites/common.sh
++ WORK_DIR=/tmp/frugalos_it
+ CLUSTER=three-nodes
+ docker-compose -f it/clusters/three-nodes.yml down
...
...
...
Stopping clusters_frugalos02_1 ... done
Stopping clusters_frugalos03_1 ... done
Stopping clusters_frugalos01_1 ... done
Removing clusters_frugalos02_1 ... done
Removing clusters_frugalos03_1 ... done
Removing clusters_frugalos01_1 ... done
Removing network clusters_frugalos_net
```

特にエラーメッセージの類が出力されていない場合はテストにパスしたことを意味する。

参考
-----

- Pumba:
 - https://github.com/gaia-adm/pumba
 - https://github.com/alexei-led/blog/blob/master/content/post/pumba_docker_chaos_testing.md