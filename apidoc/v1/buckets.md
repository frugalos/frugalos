FORMAT: 1A
HOST: http://example.com

# バケツAPI (Version 1)

バケツ関連の操作を提供するAPI。

## 用語

<!-- include(../terminology.md) -->


## 共通仕様

<!-- include(../error_response.md) -->


## Data Structures

### BucketReplicationType (enum[string])

+ `dispersed` - データをErasureCodingを用いて冗長化する
+ `replicated` - データを複製によって冗長化する
+ `metadata` - データを複製によって冗長化する。またデータはオンメモリに保持される。

### BucketCore

+ id: chunk (string, required) - バケツのID
+ device: device00 (string, required) - バケツの中身(オブジェクト群)を保存するのに使用するデバイスのID

### Bucket

+ Include BucketCore
+ seqno: 0 (number, required) - バケツ内部で自動採番されている数値。利用者が意識する必要はない。
+ segment_count: 1000 (number, optional) - セグメント数 (正の整数)
  + Default: 1000
+ tolerable_faults: 3 (number, required) - 故障耐性数 (非負の整数)

### TaggedBucket

+ One Of
  + dispersed (DispersedBucket)
  + replicated (ReplicatedBucket)
  + metadata (MetadataBucket)

### BucketListItem

+ Include BucketCore
+ type: dispersed (BucketReplicationType, required) - バケツ内のデータの冗長化方式

### MetadataBucket

+ Include Bucket

### ReplicatedBucket

+ Include Bucket

### DispersedBucket

+ data_fragment_count (number, required) - ErasureCodingにおけるデータフラグメントの数(正の整数)
+ Include Bucket

### Segment

+ id: 0 (number, required) - セグメントのID


### ObjectSummary

+ id: sm9 (string, required) - オブジェクトのID
+ version: 3 (number, required) - オブジェクトのバージョン

<!-- include(../data_structures.md) -->

# Group バケツ

## バケツ一覧 [/v1/buckets]

### バケツ一覧の取得 [GET]

存在するバケツのID一覧を返す。

+ Response 200 (application/json)

  + Body

            [
                {
                    "id": "vod_chunk",
                    "type": "dispersed",
                    "device": "root"
                }
            ]

  + Attributes (array[BucketListItem], fixed-type, required)

## バケツ操作 [/v1/buckets/{bucket_id}]

+ Parameters
  + bucket_id: `foo` (string, required) - 操作対象のバケツのID

### バケツ構成の取得 [GET]

指定されたバケツの構成を取得する。

+ Response 200 (application/json)

  + Body

            {
                "dispersed": {
                    "id": "vod_chunk",
                    "seqno": 0,
                    "device": "root",
                    "segment_count": 43,
                    "tolerable_faults": 1,
                    "data_fragment_count": 2
                }
            }

  + Attributes (TaggedBucket, required)

+ Response 404 (application/problem+json)

  指定されたバケツが存在しない。

  + Attributes (Problem, required)

### バケツの作成・更新 [PUT]

バケツの新規作成、あるいは更新を行う。

+ Request (application/json)
  + Attributes (TaggedBucket, required)

+ Response 200 (application/json)
  バケツが更新された。

  + Attributes (TaggedBucket, required)

+ Response 201 (application/json)
  バケツが作成された。

  + Attributes (TaggedBucket, required)

+ Response 400 (application/problem+json)
  指定されたバケツの構成が不正だったり、対象のバケツが更新をサポートしていない場合に返される。
  なお、更新をサポートしているのは、種別が`metadata`のバケツのみである。

  + Attributes (Problem, required)

### バケツの削除（未実装） [DELETE]

指定されたバケツを削除する。

+ Response 200 (application/json)
  削除されたバケツの構成を返す。

  + Attributes (Bucket, required)

# Group オブジェクト

## オブジェクト操作 [/v1/buckets/{bucket_id}/objects/{object_id}{?deadline,expect}]

個々のオブジェクトに対する操作。HTTP ヘッダーで `If-None`, `If-None-Match` のいずれも指定しなかった場合はオブジェクトのバージョン確認は**されない**。

指定可能な HTTP Header は以下のとおり。

* `If-None`
    * オブジェクトのバージョンがこのヘッダーで指定した値の場合のみ操作が適用される。
    * `*` を指定すると任意のバージョンに対して操作が適用される。
* `If-None-Match`
    * オブジェクトのバージョンがこのヘッダーで指定した値以外の場合のみ操作が適用される。
    * `*` を指定するとオブジェクトが存在しない場合のみ操作が適用される。

+ Parameters
  + bucket_id: foo (string, required) - 操作対象のバケツのID
  + object_id: bar (string, required) - 操作対象のオブジェクトのID
  + deadline: 5000 (number, optional) - 処理完了までのデッドライン(ミリ秒)
      + Default: 5000

### オブジェクトの取得 [GET]

`object_id`で指定されたオブジェクトの内容を取得する。

### 注記

+ Response 200 (application/octet-stream)
  以前にPUTされたオブジェクトの内容を取得する。

  応答ヘッダの`ETag`には、該当オブジェクトのバージョンが格納される。

  + Headers

            ETag: 10

  + Body

            ${オブジェクトの内容}

+ Response 404 (application/problem+json)

  対象オブジェクトが存在しない。

  + Attributes (Problem, required)

+ Response 410 (application/problem+json)
  `object_id`に対応するオブジェクトは存在するが、内容が壊れている。

  再び利用可能にするには、ユーザが再度PUTを発行する必要がある可能性が高い。

  応答ヘッダの`ETag`には、該当オブジェクトのバージョンが格納される。

  + Headers

            ETag: 10

  + Attributes (Problem, required)

### オブジェクトの存在確認 [HEAD]

`object_id`で指定されたオブジェクトが存在するかを確認する。

### 注記

+ Response 200 (application/octet-stream)

  応答ヘッダの`ETag`には、該当オブジェクトのバージョンが格納される。

  + Headers

            ETag: 10

+ Response 404 (application/problem+json)

  対象オブジェクトが存在しない。

  + Attributes (Problem, required)


### オブジェクトの作成・更新 [PUT]

オブジェクトの新規作成、あるいは更新を行う。

+ Request (application/octet-stream)
  + Body

            ${オブジェクトの内容}

+ Response 200
  オブジェクトが更新された。

  応答ヘッダの`ETag`には、更新後のオブジェクトのバージョンが格納される。

  + Headers

            ETag: 10

+ Response 201
  オブジェクトが作成された。

  応答ヘッダの`ETag`には、作成後のオブジェクトのバージョンが格納される。

  + Headers

            ETag: 10

+ Response 412 (application/problem+json)
  `expect`パラメータで指定された条件と、実際のオブジェクトのバージョンが異なる。

  応答ヘッダの`ETag`には、現在のオブジェクトのバージョンが格納される。
  なお、オブジェクトが存在しない場合には、このヘッダは応答には含まれない。

  + Headers

            ETag: 10

  + Attributes (Problem, required)

### オブジェクトの削除 [DELETE]

オブジェクトの削除を行う。

対象オブジェクトが存在しない場合には、`404`応答が返される。

+ Response 200
  オブジェクトが削除された。

  応答ヘッダの`ETag`には、削除されたオブジェクトのバージョンが格納される。

  + Headers

            ETag: 10

+ Response 404 (application/problem+json)

  対象オブジェクトが存在しない。

  + Attributes (Problem, required)

+ Response 412 (application/problem+json)
  `expect`パラメータで指定された条件と、実際のオブジェクトのバージョンが異なる。

  応答ヘッダの`ETag`には、現在のオブジェクトのバージョンが格納される。
  なお、オブジェクトが存在しない場合には、このヘッダは応答には含まれない。

  + Headers

            ETag: 10

  + Attributes (Problem, required)


# Group オブジェクトプレフィックス

## プレフィックス指定でのオブジェクト操作 [/v1/buckets/{bucket_id}/object_prefixes/{object_prefix}]

### オブジェクトの削除 [DELETE]

指定したプレフィックスを ID のプレフィックスに持つオブジェクトを削除する。

例えば、`lv123`, `25lv25` の ID を持つオブジェクトが登録されている場合に、`lv` を `object_prefix` に指定すると、`lv123` のみ削除される。

削除されたオブジェクトが1つもなくても `200` 応答が返される。

+ Response 200
    オブジェクトが削除された。

    + Attributes (object)
        + total (string) - 削除されたオブジェクト総数の文字列表現(JSON で表現できない巨大な数になりうるため)

    + Body

            {
                "total": "10"
            }

+ Parameters
    + bucket_id: `live` (string, required) - 操作対象のバケツのID
    + object_prefix: `lv3293` (string, required) - 削除対象のオブジェクトのIDのプレフィックス

# Group セグメント

## セグメント一覧 [/v1/buckets/{bucket_id}/segments]

+ Parameters
  + bucket_id: `foo` (string, required) - 操作対象のバケツのID

### セグメント一覧の取得 [GET]

指定されたバケツのセグメント一覧を取得する。

+ Response 200 (application/json)
    + Body

             [
                 {"id": 0},
                 {"id": 1},
                 {"id": 2}
             ]

    + Attributes (array[Segment], fixed-type, required)

+ Response 404 (application/problem+json)

  対象のバケツが存在しない。

  + Attributes (Problem, required)

## セグメント操作 [/v1/buckets/{bucket_id}/segments/{segment_id}]

+ Parameters
  + bucket_id: `foo` (string, required) - 操作対象のバケツのID
  + segment_id: `0` (number, required) - 操作対象のセグメントのID

### セグメント構成の取得(未実装) [GET]

指定されたセグメントの構成情報を返す。

TODO: 各デバイスに関して、データ同期が完了(i.e., 即座に利用可能か)しているかどうかのフラグを返しても良いかも
(statistics的なもので「Raftログのcommit済み地点」を返せば十分？)

### 注記

応答の値は暫定。

+ Response 200 (application/json)
    + Body

            {
                "devices": ["device00", "device01"]
            }

## オブジェクト一覧 [/v1/buckets/{bucket_id}/segments/{segment_id}/objects]

+ Parameters
  + bucket_id: `foo` (string, required) - 操作対象のバケツのID
  + segment_id: `0` (number, required) - 操作対象のセグメントのID

### オブジェクト一覧の取得 [GET]

指定されたセグメントに属するオブジェクトの一覧を返す。

+ Response 200 (application/json)
  + Body

            [
                {"id": "object_a", "version": 100},
                {"id": "object_b", "version": 3},
                {"id": "object_c", "version": 12}
            ]

  + Attributes (array[ObjectSummary], fixed-type, required)

+ Response 400 (application/problem+json)

  セグメントIDが不正。セグメントIDに適切な型を指定しているか確認する。

  + Attributes (Problem, required)

+ Response 404 (application/problem+json)

  対象のセグメントが存在しない。存在しないバケツIDが指定された場合に返される。

  + Attributes (Problem, required)

+ Response 500 (application/problem+json)

  バケツは存在するがセグメントIDが存在するセグメント範囲を超えている。現在は実装の都合で 500 を返すが、将来的には 400 を返すように修正される予定。

  + Attributes (Problem, required)
