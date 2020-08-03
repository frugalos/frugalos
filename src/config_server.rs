use bytecodec::json_codec::{JsonDecoder, JsonEncoder};
use bytecodec::null::NullDecoder;
use cannyls_rpc::DeviceId;
use fibers_http_server::{HandleRequest, Reply, Req, ServerBuilder as HttpServerBuilder, Status};
use fibers_rpc::client::ClientServiceHandle as RpcServiceHandle;
use futures::future::Either;
use futures::Future;
use httpcodec::{BodyDecoder, BodyEncoder};
use libfrugalos::client::config::Client as ConfigRpcClient;
use libfrugalos::entity::bucket::{Bucket, BucketSummary};
use libfrugalos::entity::device::{Device, DeviceSummary};
use libfrugalos::entity::server::{Server, ServerSummary};
use std::net::SocketAddr;
use url::Url;

use crate::daemon::FrugalosDaemonHandle;
use crate::http::{make_json_response, not_found, HttpResult};
use crate::{Error, Result};

#[derive(Clone)]
pub struct ConfigServer {
    rpc_service: RpcServiceHandle,
    local_addr: SocketAddr,
    daemon_handle: FrugalosDaemonHandle,
}
impl ConfigServer {
    pub fn new(
        rpc_service: RpcServiceHandle,
        local_addr: SocketAddr,
        daemon_handle: FrugalosDaemonHandle,
    ) -> Self {
        ConfigServer {
            rpc_service,
            local_addr,
            daemon_handle,
        }
    }
    pub fn register(self, builder: &mut HttpServerBuilder) -> Result<()> {
        track!(builder.add_handler(ListServers(self.clone())))?;
        track!(builder.add_handler(PutServer(self.clone())))?;
        track!(builder.add_handler(GetServer(self.clone())))?;

        track!(builder.add_handler(ListDevices(self.clone())))?;
        track!(builder.add_handler(PutDevice(self.clone())))?;
        track!(builder.add_handler(GetDevice(self.clone())))?;
        track!(builder.add_handler(PutDeviceState(self.clone())))?;

        track!(builder.add_handler(ListBuckets(self.clone())))?;
        track!(builder.add_handler(PutBucket(self.clone())))?;
        track!(builder.add_handler(GetBucket(self.clone())))?;
        track!(builder.add_handler(DeleteBucket(self.clone())))?;

        // 上の clone を一つだけ消したくないので、ここで drop する
        drop(self);
        Ok(())
    }
    fn client(&self) -> ConfigRpcClient {
        ConfigRpcClient::new(self.local_addr, self.rpc_service.clone())
    }
}

struct ListServers(ConfigServer);
impl HandleRequest for ListServers {
    const METHOD: &'static str = "GET";
    const PATH: &'static str = "/v1/servers";

    type ReqBody = ();
    type ResBody = HttpResult<Vec<ServerSummary>>;
    type Decoder = BodyDecoder<NullDecoder>;
    type Encoder = BodyEncoder<JsonEncoder<Self::ResBody>>;
    type Reply = Reply<Self::ResBody>;

    fn handle_request(&self, _req: Req<Self::ReqBody>) -> Self::Reply {
        let future = self.0.client().list_servers().then(|result| {
            let (status, body) = match track!(result) {
                Err(e) => (Status::InternalServerError, Err(Error::from(e))),
                Ok(v) => (Status::Ok, Ok(v)),
            };
            Ok(make_json_response(status, body))
        });
        Box::new(future)
    }
}

struct PutServer(ConfigServer);
impl HandleRequest for PutServer {
    const METHOD: &'static str = "PUT";
    const PATH: &'static str = "/v1/servers/*";

    type ReqBody = Server;
    type ResBody = HttpResult<Server>;
    type Decoder = BodyDecoder<JsonDecoder<Self::ReqBody>>;
    type Encoder = BodyEncoder<JsonEncoder<Self::ResBody>>;
    type Reply = Reply<Self::ResBody>;

    fn handle_request(&self, req: Req<Self::ReqBody>) -> Self::Reply {
        let server = req.into_body();
        let future = self.0.client().put_server(server).then(|result| {
            let (status, body) = match track!(result) {
                Err(e) => (Status::InternalServerError, Err(Error::from(e))),
                Ok(v) => (Status::Ok, Ok(v)),
            };
            Ok(make_json_response(status, body))
        });
        Box::new(future)
    }
}

struct GetServer(ConfigServer);
impl HandleRequest for GetServer {
    const METHOD: &'static str = "GET";
    const PATH: &'static str = "/v1/servers/*";

    type ReqBody = ();
    type ResBody = HttpResult<Server>;
    type Decoder = BodyDecoder<NullDecoder>;
    type Encoder = BodyEncoder<JsonEncoder<Self::ResBody>>;
    type Reply = Reply<Self::ResBody>;

    fn handle_request(&self, req: Req<Self::ReqBody>) -> Self::Reply {
        let server_id = get_id(&req.url());
        let future = self.0.client().get_server(server_id).then(|result| {
            let (status, body) = match track!(result) {
                Err(e) => (Status::InternalServerError, Err(Error::from(e))),
                Ok(None) => (Status::NotFound, Err(track!(not_found()))),
                Ok(Some(v)) => (Status::Ok, Ok(v)),
            };
            Ok(make_json_response(status, body))
        });
        Box::new(future)
    }
}

struct ListDevices(ConfigServer);
impl HandleRequest for ListDevices {
    const METHOD: &'static str = "GET";
    const PATH: &'static str = "/v1/devices";

    type ReqBody = ();
    type ResBody = HttpResult<Vec<DeviceSummary>>;
    type Decoder = BodyDecoder<NullDecoder>;
    type Encoder = BodyEncoder<JsonEncoder<Self::ResBody>>;
    type Reply = Reply<Self::ResBody>;

    fn handle_request(&self, _req: Req<Self::ReqBody>) -> Self::Reply {
        let future = self.0.client().list_devices().then(|result| {
            let (status, body) = match track!(result) {
                Err(e) => (Status::InternalServerError, Err(Error::from(e))),
                Ok(v) => (Status::Ok, Ok(v)),
            };
            Ok(make_json_response(status, body))
        });
        Box::new(future)
    }
}

struct PutDevice(ConfigServer);
impl HandleRequest for PutDevice {
    const METHOD: &'static str = "PUT";
    const PATH: &'static str = "/v1/devices/*";

    type ReqBody = Device;
    type ResBody = HttpResult<Device>;
    type Decoder = BodyDecoder<JsonDecoder<Self::ReqBody>>;
    type Encoder = BodyEncoder<JsonEncoder<Self::ResBody>>;
    type Reply = Reply<Self::ResBody>;

    fn handle_request(&self, req: Req<Self::ReqBody>) -> Self::Reply {
        let device = req.into_body();
        let future = self.0.client().put_device(device).then(|result| {
            let (status, body) = match track!(result) {
                Err(e) => {
                    if let libfrugalos::ErrorKind::InvalidInput = e.kind() {
                        (Status::BadRequest, Err(Error::from(e)))
                    } else {
                        (Status::InternalServerError, Err(Error::from(e)))
                    }
                }
                Ok(v) => (Status::Ok, Ok(v)),
            };
            Ok(make_json_response(status, body))
        });
        Box::new(future)
    }
}

struct GetDevice(ConfigServer);
impl HandleRequest for GetDevice {
    const METHOD: &'static str = "GET";
    const PATH: &'static str = "/v1/devices/*";

    type ReqBody = ();
    type ResBody = HttpResult<Device>;
    type Decoder = BodyDecoder<NullDecoder>;
    type Encoder = BodyEncoder<JsonEncoder<Self::ResBody>>;
    type Reply = Reply<Self::ResBody>;

    fn handle_request(&self, req: Req<Self::ReqBody>) -> Self::Reply {
        let device_id = get_id(&req.url());
        let future = self.0.client().get_device(device_id).then(|result| {
            let (status, body) = match track!(result) {
                Err(e) => (Status::InternalServerError, Err(Error::from(e))),
                Ok(None) => (Status::NotFound, Err(track!(not_found()))),
                Ok(Some(v)) => (Status::Ok, Ok(v)),
            };
            Ok(make_json_response(status, body))
        });
        Box::new(future)
    }
}

#[derive(Serialize, Deserialize)]
struct DeviceState {
    state: RunningState,
}

#[derive(Serialize, Deserialize, Copy, Clone)]
#[serde(rename_all = "lowercase")]
enum RunningState {
    Started,
    Stopped,
}

struct PutDeviceState(ConfigServer);
impl HandleRequest for PutDeviceState {
    const METHOD: &'static str = "PUT";
    const PATH: &'static str = "/v1/devices/*/state";

    type ReqBody = DeviceState;
    type ResBody = HttpResult<DeviceState>;
    type Decoder = BodyDecoder<JsonDecoder<Self::ReqBody>>;
    type Encoder = BodyEncoder<JsonEncoder<Self::ResBody>>;
    type Reply = Reply<Self::ResBody>;

    fn handle_request(&self, req: Req<Self::ReqBody>) -> Self::Reply {
        let device_id = get_id(&req.url());
        let device_state = req.into_body();
        let client = self.0.client();
        let daemon_handle = self.0.daemon_handle.clone();
        let future = client
            .get_device(device_id)
            .then(move |result| {
                let (status, body) = match track!(result) {
                    Err(e) => (Status::InternalServerError, Err(Error::from(e))),
                    Ok(None) => (Status::NotFound, Err(track!(not_found()))),
                    Ok(Some(v)) => (Status::Ok, Ok(v)),
                };
                futures::future::ok((status, body))
            })
            .and_then(move |(status, body)| match (body, device_state.state) {
                (Err(e), _) => Either::A(futures::future::ok((status, Err(e)))),
                (Ok(_device), RunningState::Started) => {
                    Either::A(futures::future::ok((status, Ok(device_state))))
                }
                (Ok(device), RunningState::Stopped) => {
                    let device_seqno = device.seqno();
                    let device_id = DeviceId::new(device.id());
                    let future =
                        daemon_handle
                            .stop_device(device_seqno, device_id)
                            .map(move |result| {
                                if result {
                                    (status, Ok(device_state))
                                } else {
                                    (Status::NotFound, Err(track!(not_found())))
                                }
                            });
                    Either::B(future)
                }
            })
            .then(move |result| {
                let result = match result {
                    Ok((status, body)) => make_json_response(status, body),
                    Err(e) => make_json_response(Status::InternalServerError, Err(e)),
                };
                Ok(result)
            });
        Box::new(future)
    }
}

struct GetDeviceState(ConfigServer);
impl HandleRequest for GetDeviceState {
    const METHOD: &'static str = "GET";
    const PATH: &'static str = "/v1/devices/*/state";

    type ReqBody = ();
    type ResBody = HttpResult<DeviceState>;
    type Decoder = BodyDecoder<NullDecoder>;
    type Encoder = BodyEncoder<JsonEncoder<Self::ResBody>>;
    type Reply = Reply<Self::ResBody>;

    fn handle_request(&self, req: Req<Self::ReqBody>) -> Self::Reply {
        let _device_id = get_id(&req.url());
        let device_state = DeviceState {
            state: RunningState::Started,
        };
        // TODO
        let future = futures::future::ok(make_json_response(Status::Ok, Ok(device_state)));
        Box::new(future)
    }
}

struct ListBuckets(ConfigServer);
impl HandleRequest for ListBuckets {
    const METHOD: &'static str = "GET";
    const PATH: &'static str = "/v1/buckets";

    type ReqBody = ();
    type ResBody = HttpResult<Vec<BucketSummary>>;
    type Decoder = BodyDecoder<NullDecoder>;
    type Encoder = BodyEncoder<JsonEncoder<Self::ResBody>>;
    type Reply = Reply<Self::ResBody>;

    fn handle_request(&self, _req: Req<Self::ReqBody>) -> Self::Reply {
        let future = self.0.client().list_buckets().then(|result| {
            let (status, body) = match track!(result) {
                Err(e) => (Status::InternalServerError, Err(Error::from(e))),
                Ok(v) => (Status::Ok, Ok(v)),
            };
            Ok(make_json_response(status, body))
        });
        Box::new(future)
    }
}

struct PutBucket(ConfigServer);
impl HandleRequest for PutBucket {
    const METHOD: &'static str = "PUT";
    const PATH: &'static str = "/v1/buckets/*";

    type ReqBody = Bucket;
    type ResBody = HttpResult<Bucket>;
    type Decoder = BodyDecoder<JsonDecoder<Self::ReqBody>>;
    type Encoder = BodyEncoder<JsonEncoder<Self::ResBody>>;
    type Reply = Reply<Self::ResBody>;

    fn handle_request(&self, req: Req<Self::ReqBody>) -> Self::Reply {
        let bucket = req.into_body();
        let future = self.0.client().put_bucket(bucket).then(|result| {
            let (status, body) = match track!(result) {
                Err(e) => {
                    if let libfrugalos::ErrorKind::InvalidInput = e.kind() {
                        (Status::BadRequest, Err(Error::from(e)))
                    } else {
                        (Status::InternalServerError, Err(Error::from(e)))
                    }
                }
                Ok(v) => (Status::Ok, Ok(v)),
            };
            Ok(make_json_response(status, body))
        });
        Box::new(future)
    }
}

struct GetBucket(ConfigServer);
impl HandleRequest for GetBucket {
    const METHOD: &'static str = "GET";
    const PATH: &'static str = "/v1/buckets/*";

    type ReqBody = ();
    type ResBody = HttpResult<Bucket>;
    type Decoder = BodyDecoder<NullDecoder>;
    type Encoder = BodyEncoder<JsonEncoder<Self::ResBody>>;
    type Reply = Reply<Self::ResBody>;

    fn handle_request(&self, req: Req<Self::ReqBody>) -> Self::Reply {
        let bucket_id = get_id(&req.url());
        let future = self.0.client().get_bucket(bucket_id).then(|result| {
            let (status, body) = match track!(result) {
                Err(e) => (Status::InternalServerError, Err(Error::from(e))),
                Ok(None) => (Status::NotFound, Err(track!(not_found()))),
                Ok(Some(v)) => (Status::Ok, Ok(v)),
            };
            Ok(make_json_response(status, body))
        });
        Box::new(future)
    }
}

struct DeleteBucket(ConfigServer);
impl HandleRequest for DeleteBucket {
    const METHOD: &'static str = "DELETE";
    const PATH: &'static str = "/v1/buckets/*";

    type ReqBody = ();
    type ResBody = HttpResult<Option<Bucket>>;
    type Decoder = BodyDecoder<NullDecoder>;
    type Encoder = BodyEncoder<JsonEncoder<Self::ResBody>>;
    type Reply = Reply<Self::ResBody>;

    fn handle_request(&self, req: Req<Self::ReqBody>) -> Self::Reply {
        let bucket_id = get_id(&req.url());
        let future = self.0.client().delete_bucket(bucket_id).then(|result| {
            let (status, body) = match track!(result) {
                Err(e) => (Status::InternalServerError, Err(Error::from(e))),
                Ok(None) => (Status::NotFound, Err(not_found())),
                Ok(Some(v)) => (Status::Ok, Ok(Some(v))),
            };
            Ok(make_json_response(status, body))
        });
        Box::new(future)
    }
}

fn get_id(url: &Url) -> String {
    url.path_segments()
        .expect("Never fails")
        .nth(2)
        .expect("Never fails")
        .to_string()
}
