frugalos
========

[![Crates.io: frugalos](https://img.shields.io/crates/v/frugalos.svg)](https://crates.io/crates/frugalos)
[![Documentation](https://docs.rs/frugalos/badge.svg)](https://docs.rs/frugalos)
[![Build Status](https://travis-ci.org/frugalos/frugalos.svg?branch=master)](https://travis-ci.org/frugalos/frugalos)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Frugal Object Storage


Documentation
-------------

- [Rustdoc](https://docs.rs/frugalos)
- [Wiki (Japanese only)](https://github.com/frugalos/frugalos/wiki)


Installation
------------

You can install `frugalos` by executing the following command:
```console
$ cargo install frugalos
```

Below is a minimal usage example:
```console
// Creates a cluster.
$ frugalos create --id example --data-dir example/

// Starts a frugalos process in the background.
$ frugalos start --data-dir example/ &

// Add a device and a bucket for storing objects.
$ DEVICE_JSON='{"file": {"id": "file0", "server": "example", "filepath": "example/file0.lusf"}}'
$ curl -XPUT -d "$DEVICE_JSON" http://localhost:3000/v1/devices/file0

$ BUCKET_JSON='{"metadata": {"id": "bucket0", "device": "file0", "tolerable_faults": 1}}'
$ curl -XPUT -d "$BUCKET_JSON" http://localhost:3000/v1/buckets/bucket0

// PUT and GET objects
$ curl -XPUT -d 'foo' http://localhost:3000/v1/buckets/bucket0/objects/foo
$ curl http://localhost:3000/v1/buckets/bucket0/objects/foo
foo
```

Please see [REST API] for details and other available APIs.

[REST API]: https://github.com/frugalos/frugalos/wiki/REST-API


For Frugalos Developers
-----------------------

Please see [Developer's Guide].

[Developer's Guide]: https://github.com/frugalos/frugalos/wiki/Developer%27s-Guide
