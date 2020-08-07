use atomic_immut::AtomicImmut;
use byteorder::{BigEndian, ByteOrder};
use cannyls;
use cannyls::deadline::Deadline;
use cannyls::device::{Device, DeviceHandle};
use cannyls::lump::LumpId;
use cannyls_rpc::DeviceId;
use cannyls_rpc::DeviceRegistryHandle;
use fibers::sync::oneshot;
use fibers::Spawn;
use fibers_rpc::client::ClientServiceHandle as RpcServiceHandle;
use fibers_rpc::server::ServerBuilder as RpcServerBuilder;
use fibers_tasque;
use fibers_tasque::TaskQueueExt;
use frugalos_config::{DeviceGroup, Event as ConfigEvent, Service as ConfigService};
use frugalos_core::tracer::ThreadLocalTracer;
use frugalos_mds;
use frugalos_raft::{NodeId, Service as RaftService};
use frugalos_segment;
use frugalos_segment::FrugalosSegmentConfig;
use frugalos_segment::Service as SegmentService;
use futures::future::Fuse;
use futures::{Async, Future, Poll, Stream};
use libfrugalos::entity::bucket::{Bucket as BucketConfig, BucketId};
use libfrugalos::entity::device::{
    Device as DeviceConfig, FileDevice as FileDeviceConfig, MemoryDevice as MemoryDeviceConfig,
};
use libfrugalos::entity::server::{Server, ServerId};
use prometrics::metrics::MetricBuilder;
use slog::Logger;
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use trackable::error::ErrorKindExt;

use bucket::Bucket;
use client::FrugalosClient;
use frugalos_core::lump::{LUMP_ID_NAMESPACE_OBJECT, LUMP_ID_NAMESPACE_RAFTLOG};
use recovery::RecoveryRequest;
use {DeviceBuildingConfig, Error, ErrorKind, FrugalosServiceConfig, Result};

pub struct PhysicalDevice {
    id: DeviceId,
    server: ServerId,
}

// TODO: Remove `S`
pub struct Service<S> {
    logger: Logger,
    local_server: Server,
    rpc_service: RpcServiceHandle,
    raft_service: RaftService,
    frugalos_segment_service: SegmentService<S>,
    config_service: ConfigService,

    // このサーバが所有するデバイス一覧
    local_devices: HashMap<u32, LocalDevice>,

    // クラスタ全体の情報
    seqno_to_device: HashMap<u32, PhysicalDevice>,

    buckets: Arc<AtomicImmut<HashMap<BucketId, Bucket>>>,
    bucket_no_to_id: HashMap<u32, BucketId>,

    servers: HashMap<ServerId, Server>,

    service_config: FrugalosServiceConfig,
    segment_config: FrugalosSegmentConfig,

    // 起動済みのノード一覧
    spawned_nodes: HashSet<NodeId>,

    recovery_request: Option<RecoveryRequest>,
    spawner: S,
}
impl<S> Service<S>
where
    S: Spawn + Send + Clone + 'static,
{
    // FIXME: 引数を減らす方法を考える
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        logger: Logger,
        spawner: S,
        raft_service: RaftService,
        config_service: ConfigService,
        rpc: &mut RpcServerBuilder,
        rpc_service: RpcServiceHandle,
        service_config: FrugalosServiceConfig,
        mds_config: frugalos_mds::FrugalosMdsConfig,
        segment_config: FrugalosSegmentConfig,
        recovery_request: Option<RecoveryRequest>,
        tracer: ThreadLocalTracer,
    ) -> Result<Self> {
        let frugalos_segment_service = track!(SegmentService::new(
            logger.clone(),
            spawner.clone(),
            rpc_service.clone(),
            rpc,
            raft_service.handle(),
            mds_config,
            tracer
        ))?;
        Ok(Service {
            logger,
            local_server: config_service.local_server().clone(),
            rpc_service,
            raft_service,
            frugalos_segment_service,
            config_service,

            local_devices: HashMap::new(),
            seqno_to_device: HashMap::new(),
            buckets: Arc::new(AtomicImmut::new(HashMap::new())),
            bucket_no_to_id: HashMap::new(),
            servers: HashMap::new(),
            spawned_nodes: HashSet::new(),
            recovery_request,
            service_config,
            segment_config,
            spawner,
        })
    }
    pub fn client(&self) -> FrugalosClient {
        FrugalosClient::new(self.buckets.clone())
    }
    pub fn stop(&mut self) {
        self.frugalos_segment_service.stop();
    }
    pub fn take_snapshot(&mut self) {
        self.frugalos_segment_service.take_snapshot();
    }
    pub fn truncate_bucket(&mut self, bucket_seqno: u32) {
        // バケツが存在する場合はバケツ削除処理自体が要求されていない可能性があり
        // 意図しない raft クラスタの破壊を防ぐため処理を実施しない
        if self.bucket_no_to_id.contains_key(&bucket_seqno) {
            return;
        }
        let raftlog_delete_range =
            make_truncate_bucket_range(LUMP_ID_NAMESPACE_RAFTLOG, bucket_seqno);
        let object_delete_range =
            make_truncate_bucket_range(LUMP_ID_NAMESPACE_OBJECT, bucket_seqno);
        let logger = self.logger.clone();
        let logger = logger.new(o!(
            "bucket_seqno" => format!("{}", bucket_seqno),
            "raftlog_delete_range" => format!("{:?}", raftlog_delete_range),
            "object_delete_range" => format!("{:?}", object_delete_range),
        ));
        let logger1 = logger.clone();

        let futures: Vec<_> = self
            .local_devices
            .values_mut()
            .map(|local_device| local_device.watch())
            .map(|device_handle| {
                let future = Box::new(device_handle.map_err(|e| track!(e)));
                let raftlog_delete_range = raftlog_delete_range.clone();
                let object_delete_range = object_delete_range.clone();
                future
                    .and_then(move |device| {
                        device
                            .request()
                            .deadline(Deadline::Infinity)
                            .wait_for_running()
                            .delete_range(raftlog_delete_range)
                            .map(|_| device)
                            .map_err(|e| track!(Error::from(e)))
                    })
                    .and_then(move |device| {
                        device
                            .request()
                            .deadline(Deadline::Infinity)
                            .wait_for_running()
                            .delete_range(object_delete_range)
                            .map_err(|e| track!(Error::from(e)))
                    })
            })
            .collect();
        let future = futures::future::join_all(futures)
            .map(move |_| {
                info!(logger, "Finish truncate_bucket");
            })
            .map_err(move |e| error!(logger1, "Error: {}", e));
        self.spawner.spawn(future);
    }

    fn handle_config_event(&mut self, event: ConfigEvent) -> Result<()> {
        info!(self.logger, "Configuration Event: {:?}", event);
        match event {
            ConfigEvent::PutDevice(device) => {
                if let Some(server) = device.server() {
                    let d = PhysicalDevice {
                        id: DeviceId::new(device.id().clone()),
                        server: server.clone(),
                    };
                    self.seqno_to_device.insert(device.seqno(), d);
                }
                if device
                    .server()
                    .map_or(false, |s| *s == self.local_server.id)
                {
                    track!(self.spawn_device(&device))?;
                }
            }
            ConfigEvent::DeleteDevice(device) => {
                self.seqno_to_device.remove(&device.seqno());
                // TODO:
                track_panic!(ErrorKind::Other, "Unsupported: {:?}", device);
            }
            ConfigEvent::PutBucket(bucket) => {
                track!(self.handle_put_bucket(&bucket))?;
            }
            ConfigEvent::DeleteBucket(bucket) => {
                track!(self.handle_delete_bucket(&bucket))?;
            }
            ConfigEvent::PatchSegment {
                bucket_no,
                segment_no,
                groups,
            } => {
                track_assert_eq!(groups.len(), 1, ErrorKind::Other, "Unimplemented");
                track!(self.handle_patch_segment(bucket_no, segment_no, &groups[0]))?;
            }
            ConfigEvent::DeleteSegment {
                bucket_no,
                segment_no,
                groups,
            } => {
                track_assert_eq!(groups.len(), 1, ErrorKind::Other, "Unimplemented");
                track!(self.handle_delete_segment(bucket_no, segment_no, &groups[0]))?;
            }
            ConfigEvent::PutServer(server) => {
                self.servers.insert(server.id.clone(), server);
            }
            ConfigEvent::DeleteServer(server) => {
                self.servers.remove(&server.id);
            }
        }
        Ok(())
    }
    fn handle_put_bucket(&mut self, bucket_config: &BucketConfig) -> Result<()> {
        let id = bucket_config.id().clone();
        self.bucket_no_to_id
            .insert(bucket_config.seqno(), id.clone());
        let bucket = track!(Bucket::new(
            self.logger.clone(),
            self.rpc_service.clone(),
            &bucket_config,
            self.segment_config.clone(),
        ))?;
        let mut buckets = (&*self.buckets.load()).clone();
        buckets.insert(id, bucket);
        self.buckets.store(buckets);
        Ok(())
    }
    fn handle_delete_bucket(&mut self, bucket_config: &BucketConfig) -> Result<()> {
        let seqno = bucket_config.seqno();
        let id = bucket_config.id().clone();
        self.bucket_no_to_id.remove(&seqno);
        let mut buckets = (&*self.buckets.load()).clone();
        track_assert!(buckets.remove(&id).is_some(), ErrorKind::InconsistentState);
        self.buckets.store(buckets);
        Ok(())
    }
    fn handle_patch_segment(
        &mut self,
        bucket_no: u32,
        segment_no: u16,
        group: &DeviceGroup,
    ) -> Result<()> {
        // このグループに対応するRaftクラスタのメンバ群を用意
        let members = self.list_segment_members(bucket_no, segment_no, group)?;
        // バケツの更新
        if let Some(id) = self.bucket_no_to_id.get(&bucket_no) {
            // TODO: だいぶコスト高の操作なので、セグメント更新はバッチ的に行った方が良いかも
            let mut buckets = (&*self.buckets.load()).clone();
            let segment;
            {
                use frugalos_segment::config::ClusterMember;
                let bucket = buckets.get_mut(id).expect("Never fails");
                let members = members
                    .iter()
                    .zip(group.members.iter())
                    .map(|(&node, device_no)| ClusterMember {
                        node,
                        device: self.seqno_to_device[&device_no].id.clone().into_string(),
                    })
                    .collect();
                track!(bucket.update_segment(segment_no, members))?;
                segment = bucket.segments()[segment_no as usize].clone();
            }
            self.buckets.store(buckets);

            // このサーバが扱うべきRaftノードを起動
            for (&node, device_no) in members.iter().zip(group.members.iter()) {
                let device_id =
                    if let Some(id) = self.local_devices.get(&device_no).map(LocalDevice::id) {
                        id
                    } else {
                        continue;
                    };

                if self.spawned_nodes.contains(&node) {
                    info!(
                        self.logger,
                        "The node has been spawned already: {}",
                        dump!(bucket_no, segment_no, device_no, device_id, node)
                    );
                    continue;
                }

                self.spawned_nodes.insert(node);

                info!(
                    self.logger,
                    "Add a node: {}",
                    dump!(bucket_no, segment_no, device_no, device_id, node)
                );

                let device_handle = self.local_devices.get_mut(&device_no).unwrap().watch();
                track!(self.frugalos_segment_service.handle().add_node(
                    node,
                    Box::new(
                        device_handle
                            .map_err(|e| frugalos_segment::ErrorKind::Other.takes_over(e).into())
                    ),
                    segment.clone(),
                    members.iter().map(NodeId::to_raft_node_id).collect(),
                    self.recovery_request.is_some(),
                ))?;
            }
        } else {
            // 既に削除されているバケツのセグメント
            // (FIXME: タイミング的にここに来ることはなさそうなのでエラーでも良いかも)
        };

        Ok(())
    }

    fn handle_delete_segment(
        &mut self,
        bucket_no: u32,
        segment_no: u16,
        group: &DeviceGroup,
    ) -> Result<()> {
        let members = self.list_segment_members(bucket_no, segment_no, group)?;

        // 起動している raft ノードを停止する
        for (&node, device_no) in members.iter().zip(group.members.iter()) {
            if !self.spawned_nodes.contains(&node) {
                continue;
            }

            if let Some(bucket_id) = self.bucket_no_to_id.get(&bucket_no) {
                let future = track!(self.frugalos_segment_service.handle().remove_node(node))?;
                let device_handle = self.local_devices.get_mut(&device_no).unwrap().watch();
                let bucket_id = bucket_id.clone();
                let range = frugalos_segment::config::make_available_object_lump_id_range(&node);
                let logger = self.logger.clone();
                let logger = logger.new(o!(
                    "node" => node.local_id.to_string(),
                    "bucket_id" => bucket_id,
                    "bucket_no" => format!("{}", bucket_no),
                    "segment_no" => format!("{}", segment_no),
                ));
                let logger1 = logger.clone();
                let logger_end = logger.clone();

                // ノードに紐づくデータ (raftlog/オブジェクト) の削除を実行する
                //
                // データ削除をノード停止直後に実施すると並列で行われる書き込み分を見逃し
                // 削除されないデータが残ってしまう可能性があるためノード停止から一定時間経過後に実施する
                let future = future
                    .map_err(|e| track!(Error::from(e)))
                    .and_then(move |()| {
                        let waiting_time_millis =
                            std::env::var("FRUGALOS_STOP_SEGMENT_WAITING_TIME_MILLIS")
                                .map(|value| value.parse().unwrap())
                                .unwrap_or(60000);
                        let duration = Duration::from_millis(waiting_time_millis);
                        let timer = fibers::time::timer::timeout(duration);
                        timer
                            .map_err(|e| track!(Error::from(e)))
                            .and_then(|()| Box::new(device_handle))
                            .and_then(move |device| {
                                let storage = frugalos_raft::Storage::new(
                                    logger,
                                    node.local_id,
                                    device.clone(),
                                    frugalos_raft::StorageMetrics::new(),
                                );
                                let future = frugalos_raft::ClearLog::new(storage);
                                future.map(|_| device).map_err(|e| track!(Error::from(e)))
                            })
                            .and_then(move |device| {
                                device
                                    .request()
                                    .deadline(Deadline::Infinity)
                                    .wait_for_running()
                                    .delete_range(range)
                                    .map_err(|e| track!(Error::from(e)))
                                    .map(move |vec| {
                                        info!(logger1, "Delete all objects: {}", vec.len());
                                    })
                            })
                    })
                    .map_err(move |e| error!(logger_end, "HANDLE_DELETE_SEGMENT_ERROR: {}", e));
                self.spawner.spawn(future);
            }
            self.spawned_nodes.remove(&node);
        }
        Ok(())
    }

    fn spawn_device(&mut self, device_config: &DeviceConfig) -> Result<()> {
        let device = LocalDevice::new(
            self.logger.clone(),
            &device_config,
            self.service_config.device.clone(),
            self.frugalos_segment_service.device_registry().handle(),
        );
        self.local_devices.insert(device_config.seqno(), device);
        Ok(())
    }

    fn list_segment_members(
        &self,
        bucket_no: u32,
        segment_no: u16,
        group: &DeviceGroup,
    ) -> Result<Vec<NodeId>> {
        let mut members = Vec::new();
        for (member_no, device_no) in (0..group.members.len()).zip(group.members.iter()) {
            let owner = &self.servers[&self.seqno_to_device[device_no].server];
            let node: NodeId = track!(format!(
                "00{:06x}{:04x}{:02x}.{:x}@{}:{}",
                bucket_no, segment_no, member_no, device_no, owner.host, owner.port
            )
            .parse())?;
            members.push(node);
        }
        Ok(members)
    }
}
impl<S> Future for Service<S>
where
    S: Spawn + Send + Clone + 'static,
{
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        track!(self.raft_service.poll())?;
        let ready = track!(self.frugalos_segment_service.poll())?.is_ready();
        if ready {
            info!(self.logger, "Frugalos service terminated");
            return Ok(Async::Ready(()));
        }
        while let Async::Ready(event) = track!(self.config_service.poll())? {
            let event = event.expect("Never fails");
            track!(self.handle_config_event(event))?;
        }

        for device in self.local_devices.values_mut() {
            if let Err(e) = track!(device.poll()) {
                error!(self.logger, "Device error: {}", e; "device" => device.id().as_str());
            }
        }

        Ok(Async::NotReady)
    }
}

#[derive(Debug)]
struct LocalDevice {
    logger: Logger,
    config: DeviceConfig,
    device_registry: DeviceRegistryHandle,
    handle: Option<DeviceHandle>,
    future: Fuse<fibers_tasque::AsyncCall<Result<Device>>>,
    watches: Vec<oneshot::Monitored<DeviceHandle, Error>>,
}
impl LocalDevice {
    fn new(
        logger: Logger,
        config: &DeviceConfig,
        device_building_config: DeviceBuildingConfig,
        device_registry: DeviceRegistryHandle,
    ) -> Self {
        info!(logger, "Starts spawning new device: {:?}", config);
        LocalDevice {
            logger: logger.clone(),
            config: config.clone(),
            device_registry,
            handle: None,
            future: spawn_device(config, device_building_config, logger).fuse(),
            watches: Vec::new(),
        }
    }
    fn id(&self) -> DeviceId {
        DeviceId::new(self.config.id().clone())
    }
    fn watch(&mut self) -> WatchDeviceHandle {
        if let Some(ref d) = self.handle {
            WatchDeviceHandle::Ok(d.clone())
        } else {
            let (tx, rx) = oneshot::monitor();
            self.watches.push(tx);
            WatchDeviceHandle::Wait(rx)
        }
    }
}
impl Future for LocalDevice {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Async::Ready(device) = track!(self.future.poll().map_err(Error::from))? {
            info!(self.logger, "New device spawned: {:?}", self.config);
            let device = track!(device.map_err(Error::from))?;
            let handle = device.handle();
            self.handle = Some(device.handle());
            let _ = self.device_registry.put_device(self.id(), device); // TODO: handle result(?)
            for w in self.watches.drain(..) {
                w.exit(Ok(handle.clone()));
            }
        }
        Ok(Async::NotReady)
    }
}

#[derive(Debug)]
pub enum WatchDeviceHandle {
    Ok(DeviceHandle),
    Wait(oneshot::Monitor<DeviceHandle, Error>),
}
impl Future for WatchDeviceHandle {
    type Item = DeviceHandle;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match *self {
            WatchDeviceHandle::Ok(ref h) => Ok(Async::Ready(h.clone())),
            WatchDeviceHandle::Wait(ref mut f) => track!(f.poll().map_err(Error::from)),
        }
    }
}

fn spawn_device(
    device: &DeviceConfig,
    device_building_config: DeviceBuildingConfig,
    logger: Logger,
) -> fibers_tasque::AsyncCall<Result<Device>> {
    use libfrugalos::entity::device::Device;

    match *device {
        Device::Virtual(_) => {
            fibers_tasque::DefaultIoTaskQueue.async_call(|| track_panic!(ErrorKind::Other))
        }
        Device::Memory(ref d) => spawn_memory_device(d, device_building_config, logger),
        Device::File(ref d) => spawn_file_device(d, device_building_config, logger),
    }
}

fn spawn_memory_device(
    device: &MemoryDeviceConfig,
    device_building_config: DeviceBuildingConfig,
    logger: Logger,
) -> fibers_tasque::AsyncCall<Result<Device>> {
    let metrics = MetricBuilder::new()
        .label("device", device.id.as_ref())
        .clone();
    let nvm = cannyls::nvm::MemoryNvm::new(vec![0; device.capacity as usize]);
    let mut storage = cannyls::storage::StorageBuilder::new();
    storage.metrics(metrics.clone());
    fibers_tasque::DefaultIoTaskQueue.async_call(move || {
        let storage = track!(storage.create(nvm).map_err(Error::from))?;
        let device =
            make_device_builder(device_building_config, logger, metrics).spawn(|| Ok(storage)); // TODO: taskqueは止める
        Ok(device)
    })
}

fn spawn_file_device(
    device: &FileDeviceConfig,
    device_building_config: DeviceBuildingConfig,
    logger: Logger,
) -> fibers_tasque::AsyncCall<Result<Device>> {
    use cannyls::nvm::FileNvm;
    let metrics = MetricBuilder::new()
        .label("device", device.id.as_ref())
        .clone();
    let filepath = device.filepath.clone();
    let capacity = device.capacity;

    // Journal region のサイズの指定。デフォルトは 0.01。
    // TODO: file device を PUT する時に指定できるようにする
    let ratio: f64 = std::env::var("FRUGALOS_FILE_DEVICE_JOURNAL_REGION_RATIO")
        .map(|value| value.parse().unwrap())
        .unwrap_or(0.01);

    let mut storage = cannyls::storage::StorageBuilder::new();
    storage.journal_region_ratio(ratio);
    storage.metrics(metrics.clone());
    fibers_tasque::DefaultIoTaskQueue.async_call(move || {
        let (nvm, created) =
            track!(FileNvm::create_if_absent(filepath, capacity).map_err(Error::from))?;
        let storage = if created {
            track!(storage.create(nvm).map_err(Error::from))?
        } else {
            track!(storage.open(nvm).map_err(Error::from))?
        };
        let device =
            make_device_builder(device_building_config, logger, metrics).spawn(|| Ok(storage)); // TODO: taskqueは止める
        Ok(device)
    })
}

fn make_device_builder(
    device_building_config: DeviceBuildingConfig,
    logger: Logger,
    metrics: MetricBuilder,
) -> cannyls::device::DeviceBuilder {
    let mut device_builder = cannyls::device::DeviceBuilder::new();
    device_builder.logger(logger).metrics(metrics);
    if let Some(idle_threshold) = device_building_config.idle_threshold {
        device_builder.idle_threshold(idle_threshold);
    }
    if let Some(max_queue_len) = device_building_config.max_queue_len {
        device_builder.max_queue_len(max_queue_len);
    }
    if let Some(busy_threshold) = device_building_config.busy_threshold {
        device_builder.busy_threshold(busy_threshold);
    }
    if let Some(max_keep_busy_duration) = device_building_config.max_keep_busy_duration {
        device_builder.max_keep_busy_duration(max_keep_busy_duration);
    }
    device_builder.long_queue_policy(device_building_config.long_queue_policy);
    device_builder
}

fn make_truncate_bucket_range(namespace: u8, bucket_seqno: u32) -> Range<LumpId> {
    let mut id = [0; 16];
    BigEndian::write_u32(&mut id[0..4], bucket_seqno);
    id[0] = namespace;
    let start = LumpId::new(BigEndian::read_u128(&id[..]));
    BigEndian::write_u32(&mut id[0..4], bucket_seqno + 1);
    id[0] = namespace;
    let end = LumpId::new(BigEndian::read_u128(&id[..]));
    Range { start, end }
}

#[cfg(test)]
mod tests {
    use cannyls::lump::LumpId;
    use service;
    use std::ops::Range;

    #[allow(clippy::inconsistent_digit_grouping)]
    #[test]
    fn make_truncate_bucket_range_works() {
        // https://github.com/frugalos/frugalos/wiki/Naming-Rules-of-LumpIds
        // 1 byte: lump namespace
        // 7 byte: local_node_id
        //         3 byte: bucket_no
        //         2 byte: segment_no
        //         1 byte: member_no
        //         1 byte: type
        // 8 byte: index (version)

        // bucket_seqno=1 を削除する場合
        // namespace=1 かつ bucket_seqno=1 から 2 までの全てのオブジェクトを含む
        let expect = Range {
            start: LumpId::new(0x00_000001_0000_00_00_00000000_00000000u128),
            end: LumpId::new(0x00_000002_0000_00_00_00000000_00000000u128),
        };
        let actual = service::make_truncate_bucket_range(0, 1);
        assert_eq!(expect, actual);

        // bucket_seqno=1 を削除する場合は range に bucket_seqno=2 のオブジェクトが含まれない
        let another_bucket_content = LumpId::new(0x00_000002_0003_04_00_01234567_89abcdefu128);
        assert_eq!(false, actual.contains(&another_bucket_content));

        // bucket_seqno=1 を削除する場合 (namespace=1)
        let expect = Range {
            start: LumpId::new(0x01_000001_0000_00_00_00000000_00000000u128),
            end: LumpId::new(0x01_000002_0000_00_00_00000000_00000000u128),
        };
        let actual = service::make_truncate_bucket_range(1, 1);
        assert_eq!(expect, actual);
    }
}
