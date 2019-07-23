use fibers_rpc::server::{HandleCall, Reply, ServerBuilder as RpcServerBuilder};
use libfrugalos::schema::frugalos as rpc;
use ServiceHandle;

#[derive(Clone)]
pub struct RpcServer {
    service_handle: ServiceHandle,
}

impl RpcServer {
    pub fn register(service_handle: ServiceHandle, builder: &mut RpcServerBuilder) {
        let this = RpcServer { service_handle };
        builder.add_call_handler::<rpc::SetRepairIdlenessThresholdRpc, _>(this.clone());
    }
}

impl HandleCall<rpc::SetRepairIdlenessThresholdRpc> for RpcServer {
    fn handle_call(
        &self,
        repair_idleness_threshold: i64,
    ) -> Reply<rpc::SetRepairIdlenessThresholdRpc> {
        self.service_handle
            .set_repair_idleness_threshold(repair_idleness_threshold);
        Reply::done(Ok(()))
    }
}
