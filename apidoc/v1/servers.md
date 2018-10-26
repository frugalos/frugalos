FORMAT: 1A
HOST: http://example.com

# サーバAPI (Version 1)

サーバ関連の操作を提供するAPI。

リンク:
- [API定義のソースファイル](https://github.com/frugalos/frugalos/blob/master/apidoc/v1/servers.md)
- [API Blueprint](https://apiblueprint.org/)


## 用語

<!-- include(../terminology.md) -->


## 共通仕様

<!-- include(../error_response.md) -->


## Data Structures

### ServerId

+ id: server0 (string, required)

### Server

+ host: 10.0.2.10 (string, required)
+ port: 3000 (number, optional)
  + Default: 3000
+ devices: device0, device1 (array[string], optional)

<!-- include(../data_structures.md) -->


# Group サーバ

## サーバ一覧 [/v1/servers]

### サーバ一覧の取得（未実装） [GET]

クラスタに登録されているサーバのID一覧を返す。

+ Response 200 (application/json)
  + Attributes (array[ServerId], required)

## サーバ操作 [/v1/servers/{server_id}]

+ Parameters
  + server_id: `foo` (string, required) - 操作対象のサーバのID

### サーバ構成の取得（未実装） [GET]

指定されたサーバの構成を取得する。

+ Response 200 (application/json)

  + Attributes (Server, required)

### サーバの追加（未実装） [PUT]

サーバを新規に追加する。

なお、このAPIの発行によって実行されるのは、あくまでもエントリの追加のみであり、
これによって、該当ホストで自動的にfrugalosプロセスが起動する、といったことはない。

#### 注記

現在はサーバの更新には未対応なので、もし内容を変更したい場合には、一度削除してから再登録する必要がある。

+ Request (application/json)
  `devices`の値は、登録時には常に空(i.e., デフォルト値)である必要がある。

  + Attributes (Server, required)

+ Response 201 (application/json)
  サーバが追加された。

  + Attributes (Server, required)

+ Response 400 (application/problem+json)
  指定されたサーバの構成が不正だったり、既に同じIDを有するサーバが存在している場合に返される。

  + Attributes (Problem, required)

### サーバの削除（未実装） [DELETE]

指定されたサーバを削除する。

+ Response 200 (application/json)
  削除されたサーバの構成を返す。

  + Attributes (Server, required)

+ Response 400 (application/problem+json)
  指定されたサーバに属するデバイスが、いずれかのバケツによって使用されている場合に返される。

  + Attributes (Problem, required)

## 情報取得 [/v1/servers/{server_id}/]

### 統計情報の取得（未実装） [GET /v1/servers/{server_id}/statistics]

指定されたサーバの統計情報を取得する。

+ Parameters
  + server_id: `foo` (string, required) - 操作対象のサーバのID

+ Response 200 (application/json)
  + Body

            TODO
