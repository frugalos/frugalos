FORMAT: 1A
HOST: http://example.com

# デバイスAPI (Version 1)

デバイス関連の操作を提供するAPI。

リンク:
- [API定義のソースファイル](https://github.com/frugalos/frugalos/blob/master/apidoc/v1/devices.md)
- [API Blueprint](https://apiblueprint.org/)

## 用語

<!-- include(../terminology.md) -->


## 共通仕様

<!-- include(../error_response.md) -->


## Data Structures

### Device

+ weight: 1 (number, optional)
+ enable: true (boolean, optional)
  + Default: true

### PhysicalDevice

+ server: server0 (string, required)
+ Include Device

### VirtualDevice

+ type: virtual (string, fixed)
+ segment_allocate_policy: scatter (enum[string], required)
  + Members
    + scatter - "同じ子デバイスを、同一セグメントには割り当てないようにする"
    + scatter_if_possible - "可能な範囲で`scatter`を順守する"
    + neutral - "無作為に割り当てる"
    + gather - "あるセグメントには、全て同一の子デバイスを割り当てるようにする"
+ children: device0, device1 (array[string], required) - 子デバイス群
+ Include Device

### MemoryDevice

+ type: memory (string, fixed)
+ Include PhysicalDevice

### MonofileDevice

+ type: monofile (string, fixed)
+ file: /path/to/file (string, required) - オブジェクト群格納用のファイルのパス
+ capacity_gb: 5 (number, required) - GB単位のファイルサイズ(小数も可)
+ Include PhysicalDevice

<!-- include(../data_structures.md) -->


# Group デバイス

## デバイス一覧 [/v1/devices]

### デバイス一覧の取得（未実装） [GET]

オブジェクトストレージ内に存在するデバイスの一覧を返す。

+ Response 200 (application/json)

        [
            {"id": "foo", "server": "server00", type: "memory"},
            {"id": "bar", "server": "server01", type: "monofile"},
            {"id": "baz", type: "virtual"}
        ]

## デバイス操作 [/v1/devices/{device_id}]

+ Parameters
  + device_id: `foo` (string, required) - 操作対象のデバイスのID

### デバイス構成の取得（未実装） [GET]

指定されたデバイスの構成を取得する。

+ Response 200 (application/json)

  #### 注意

  以下の応答は`{"device": $オブジェクト}`形式になっているが、
  これはAPI Blueprintの表現力上の制約のためであり、
  実際の応答には`$オブジェクト`の部分のみが含まれる。
  (TODO: 実際の応答形式と一致するように修正)

  + Attributes (object, required)
      + One Of
          + device (MonofileDevice)
          + device (MemoryDevice)
          + device (VirtualDevice)

### デバイスの作成・更新（未実装） [PUT]

デバイスの新規作成、あるいは更新を行う。

+ Request (application/json)

  #### 注意

  以下では`{"device": $オブジェクト}`形式になっているが、
  これはAPI Blueprintの表現力上の制約のためであり、
  実際には`$オブジェクト`の部分のみが含まれる。
  (TODO: 実際の形式と一致するように修正)

  + Attributes (object, required)
      + One Of
          + device (VirtualDevice)
          + device (MonofileDevice)
          + device (MemoryDevice)

+ Response 200 (application/json)
  デバイスが更新された。

  #### 注意

  以下では`{"device": $オブジェクト}`形式になっているが、
  これはAPI Blueprintの表現力上の制約のためであり、
  実際には`$オブジェクト`の部分のみが含まれる。
  (TODO: 実際の形式と一致するように修正)

  + Attributes (object, required)
      + One Of
          + device (VirtualDevice)
          + device (MonofileDevice)
          + device (MemoryDevice)

+ Response 201 (application/json)
  デバイスが作成された。

  なお、このリクエストの実行によって処理されるのは、エントリの登録のみであり、
  実際に初期化処理等は、対象サーバ上で非同期に行われる。

  #### 注意

  以下では`{"device": $オブジェクト}`形式になっているが、
  これはAPI Blueprintの表現力上の制約のためであり、
  実際には`$オブジェクト`の部分のみが含まれる。
  (TODO: 実際の形式と一致するように修正)

  + Attributes (object, required)
      + One Of
          + device (VirtualDevice)
          + device (MonofileDevice)
          + device (MemoryDevice)

+ Response 400 (application/problem+json)
  指定されたデバイスの構成が不正だったり、対象のデバイスが更新をサポートしていない場合に返される。

  + Attributes (Problem, required)

### デバイスの削除（未実装） [DELETE]

指定されたデバイスを削除する。

なお、これにより削除されるのは、あくまでもエントリ情報のみであり、
例えば`monofile`で、実際に作成されたファイルが削除されることは無い。

+ Response 200 (application/json)
  削除されたデバイスの構成を返す。

  #### 注意

  以下では`{"device": $オブジェクト}`形式になっているが、
  これはAPI Blueprintの表現力上の制約のためであり、
  実際には`$オブジェクト`の部分のみが含まれる。
  (TODO: 実際の形式と一致するように修正)

  + Attributes (object, required)
      + One Of
          + device (MemoryDevice)
          + device (MonofileDevice)
          + device (VirtualDevice)

### 有効・無効の切り替え(未実装) [PUT /v1/devices/{device_id}/enable]

+ Parameters
  + device_id: `foo` (string, required) - 操作対象のデバイスのID

+ Request (application/json)
  + Attributes (boolean, required)

+ Response 200 (application/json)
  現在のフラグの値が返される。

  + Attributes (boolean, required)

## 統計情報 [/v1/devices/{device_id}/statistics]

+ Parameters
  + device_id: `foo` (string, required) - 操作対象のデバイスのID

### デバイスの統計情報の取得（未実装） [GET]

指定されたデバイスの統計情報を取得する。

+ Response 200 (application/json)

  + Body

               {
                 "type": "monofile",
                 "todo": "TODO",
                 "usage_gb": 1.0,
                 "capacity_gb": 2.5
               }
