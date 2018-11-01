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

イメージのビルド
----------------

```console
$ cp ../target/release/frugalos frugalos_image/
$ cd frugalos_image/
$ docker build -t frugalos .
$ docker run -it --rm frugalos frugalos
Usage: USAGE:
    frugalos [OPTIONS] [SUBCOMMAND]
```

クラスタの起動
---------------

```console
$ docker-compose -f clusters/three-nodes.yml up

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
$ scripts/put_devices.sh 2 frugalos01 frugalos02 frugalos03

// parity_fragments=1, data_fragments=2, でバケツを登録
$ scripts/put_buckets.sh 1 2

$ curl http://frugalos01/v1/buckets
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

参考
-----

- Pumba:
 - http://blog.terranillius.com/post/pumba_docker_chaos_testing/
 - https://github.com/gaia-adm/pumba
