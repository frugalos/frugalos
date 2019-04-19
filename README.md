<h1 align="center">Frugalos</h1>
<h3 align="center">Frugal Object Storage</h3>

<p align="center">
 <a href="https://crates.io/crates/frugalos">
 <img src="https://img.shields.io/crates/v/frugalos.svg" alt="Crates.io: frugalos">
 </a>
 <a href="https://docs.rs/frugalos">
  <img src="https://docs.rs/frugalos/badge.svg" alt="Documentation">
 </a>
 <a href="https://travis-ci.org/frugalos/frugalos">
   <img src="https://travis-ci.org/frugalos/frugalos.svg?branch=master" alt="Build Status">
 </a>
 <a href="LICENSE">
  <img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License: MIT">
 </a>
</p>

Frugalos is a distributed object storage written by Rust.  
It is suitable for storing medium size BLOBs that become petabyte scale in total.

Documentation
-------------

- [Rustdoc](https://docs.rs/frugalos)
- [Wiki (Japanese version only)](https://github.com/frugalos/frugalos/wiki)
- [Real World Example in Niconico (used for recording live video streams)][niconico example]

[niconico example]: https://dwango.github.io/articles/frugalos/

Installation
------------

You can install `frugalos` by the following command:
```console
$ cargo install frugalos
```

**Note:** The current installation process requires `automake`, `autoconf`, and `libtool` to build [liberasurecode] internally. If you have not installed them, please install them. (See also [liberasurecode's prerequisites])

You can also use pre-build binaries from the [releases] page.

[liberasurecode]: https://github.com/frugalos/liberasurecode
[liberasurecode's prerequisites]: https://github.com/frugalos/liberasurecode#prerequisites-to-build
[releases]: https://github.com/frugalos/frugalos/releases


Simple Example
------------
```console
// Create a cluster.
$ frugalos create --id example --data-dir example/
Oct 26 13:42:06.244 INFO [START] create: local=Server { id: "example", seqno: 0, host: V4(127.0.0.1), port: 14278 }; data_dir.as_ref()="example/"; , server: example@127.0.0.1:14278, module: frugalos_config::cluster:121
Oct 26 13:42:06.245 INFO Creates data directry: "example/", server: example@127.0.0.1:14278, module: frugalos_config::cluster:113
Oct 26 13:42:06.256 INFO [START] LoadBallot: lump_id=LumpId("03000000000000000000000000000000"); , server: example@127.0.0.1:14278, module: frugalos_raft::storage::ballot:21
...
...

// Start a frugalos process in the background.
$ frugalos start --data-dir example/ &
Oct 26 13:46:16.046 INFO Local server info: Server { id: "example", seqno: 0, host: V4(127.0.0.1), port: 14278 }, module: frugalos_config::service:68
Oct 26 13:46:16.062 INFO [START] LoadBallot: lump_id=LumpId("03000000000000000000000000000000"); , module: frugalos_raft::storage::ballot:21
Oct 26 13:46:16.086 INFO Starts RPC server, server: 127.0.0.1:14278, module: fibers_rpc::rpc_server:221
...
...

// Add a device and a bucket to store objects.
$ DEVICE_JSON='{"file": {"id": "file0", "server": "example", "filepath": "example/file0.lusf"}}'
$ curl -XPUT -d "$DEVICE_JSON" http://localhost:3000/v1/devices/file0
{"file":{"id":"file0","seqno":0,"weight":"auto","server":"example","capacity":19556691462,"filepath":"example/file0.lusf"}}%

$ BUCKET_JSON='{"metadata": {"id": "bucket0", "device": "file0", "tolerable_faults": 1}}'
$ curl -XPUT -d "$BUCKET_JSON" http://localhost:3000/v1/buckets/bucket0
{"metadata":{"id":"bucket0","seqno":0,"device":"file0","segment_count":1,"tolerable_faults":1}}%

// PUT and GET an object.
$ curl -XPUT -d 'your_object_data' http://localhost:3000/v1/buckets/bucket0/objects/your_object_id
$ curl http://localhost:3000/v1/buckets/bucket0/objects/your_object_id
your_object_data
```

Please see [REST API] for details and other available APIs.

[REST API]: https://github.com/frugalos/frugalos/wiki/REST-API


For Frugalos Developers
-----------------------

Please see [Developer's Guide].

[Developer's Guide]: https://github.com/frugalos/frugalos/wiki/Developer%27s-Guide
