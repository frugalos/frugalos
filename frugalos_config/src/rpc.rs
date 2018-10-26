use fibers_rpc::server::{HandleCall, Reply, ServerBuilder as RpcServerBuilder};
use futures::Future;
use libfrugalos::entity::bucket::{Bucket, BucketId};
use libfrugalos::entity::device::{Device, DeviceId};
use libfrugalos::entity::server::{Server, ServerId};
use libfrugalos::schema::config as spec;

use error::to_rpc_error;
use service::ServiceHandle;

/// RPC サーバ。
#[derive(Debug, Clone)]
pub struct RpcServer {
    service: ServiceHandle,
}
impl RpcServer {
    /// RPCハンドラ群を登録する。
    pub fn register(service: ServiceHandle, builder: &mut RpcServerBuilder) {
        let this = RpcServer { service };

        builder.add_call_handler::<spec::GetLeaderRpc, _>(this.clone());
        builder.add_call_handler::<spec::ListServersRpc, _>(this.clone());
        builder.add_call_handler::<spec::GetServerRpc, _>(this.clone());
        builder.add_call_handler::<spec::PutServerRpc, _>(this.clone());
        builder.add_call_handler::<spec::DeleteServerRpc, _>(this.clone());
        builder.add_call_handler::<spec::ListDevicesRpc, _>(this.clone());
        builder.add_call_handler::<spec::GetDeviceRpc, _>(this.clone());
        builder.add_call_handler::<spec::PutDeviceRpc, _>(this.clone());
        builder.add_call_handler::<spec::DeleteDeviceRpc, _>(this.clone());
        builder.add_call_handler::<spec::ListBucketsRpc, _>(this.clone());
        builder.add_call_handler::<spec::GetBucketRpc, _>(this.clone());
        builder.add_call_handler::<spec::PutBucketRpc, _>(this.clone());
        builder.add_call_handler::<spec::DeleteBucketRpc, _>(this.clone());
    }
}
impl HandleCall<spec::GetLeaderRpc> for RpcServer {
    fn handle_call(&self, _: ()) -> Reply<spec::GetLeaderRpc> {
        Reply::future(self.service.get_leader().map_err(to_rpc_error).then(Ok))
    }
}
impl HandleCall<spec::ListServersRpc> for RpcServer {
    fn handle_call(&self, _: ()) -> Reply<spec::ListServersRpc> {
        Reply::future(self.service.list_servers().map_err(to_rpc_error).then(Ok))
    }
}
impl HandleCall<spec::GetServerRpc> for RpcServer {
    fn handle_call(&self, server: ServerId) -> Reply<spec::GetServerRpc> {
        Reply::future(
            self.service
                .get_server(server)
                .map_err(to_rpc_error)
                .then(Ok),
        )
    }
}
impl HandleCall<spec::PutServerRpc> for RpcServer {
    fn handle_call(&self, server: Server) -> Reply<spec::PutServerRpc> {
        Reply::future(
            self.service
                .put_server(server)
                .map_err(to_rpc_error)
                .then(Ok),
        )
    }
}
impl HandleCall<spec::DeleteServerRpc> for RpcServer {
    fn handle_call(&self, server: ServerId) -> Reply<spec::DeleteServerRpc> {
        Reply::future(
            self.service
                .delete_server(server)
                .map_err(to_rpc_error)
                .then(Ok),
        )
    }
}
impl HandleCall<spec::ListDevicesRpc> for RpcServer {
    fn handle_call(&self, _: ()) -> Reply<spec::ListDevicesRpc> {
        Reply::future(self.service.list_devices().map_err(to_rpc_error).then(Ok))
    }
}
impl HandleCall<spec::GetDeviceRpc> for RpcServer {
    fn handle_call(&self, device: DeviceId) -> Reply<spec::GetDeviceRpc> {
        Reply::future(
            self.service
                .get_device(device)
                .map_err(to_rpc_error)
                .then(Ok),
        )
    }
}
impl HandleCall<spec::PutDeviceRpc> for RpcServer {
    fn handle_call(&self, device: Device) -> Reply<spec::PutDeviceRpc> {
        Reply::future(
            self.service
                .put_device(device)
                .map_err(to_rpc_error)
                .then(Ok),
        )
    }
}
impl HandleCall<spec::DeleteDeviceRpc> for RpcServer {
    fn handle_call(&self, device: DeviceId) -> Reply<spec::DeleteDeviceRpc> {
        Reply::future(
            self.service
                .delete_device(device)
                .map_err(to_rpc_error)
                .then(Ok),
        )
    }
}
impl HandleCall<spec::ListBucketsRpc> for RpcServer {
    fn handle_call(&self, _: ()) -> Reply<spec::ListBucketsRpc> {
        Reply::future(self.service.list_buckets().map_err(to_rpc_error).then(Ok))
    }
}
impl HandleCall<spec::GetBucketRpc> for RpcServer {
    fn handle_call(&self, bucket: BucketId) -> Reply<spec::GetBucketRpc> {
        Reply::future(
            self.service
                .get_bucket(bucket)
                .map_err(to_rpc_error)
                .then(Ok),
        )
    }
}
impl HandleCall<spec::PutBucketRpc> for RpcServer {
    fn handle_call(&self, bucket: Bucket) -> Reply<spec::PutBucketRpc> {
        Reply::future(
            self.service
                .put_bucket(bucket)
                .map_err(to_rpc_error)
                .then(Ok),
        )
    }
}
impl HandleCall<spec::DeleteBucketRpc> for RpcServer {
    fn handle_call(&self, bucket: BucketId) -> Reply<spec::DeleteBucketRpc> {
        Reply::future(
            self.service
                .delete_bucket(bucket)
                .map_err(to_rpc_error)
                .then(Ok),
        )
    }
}
