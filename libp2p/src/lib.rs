//! # Blake streams
//!
//! ## Local streams
//!
//! ```ignore
//! # use anyhow::Result;
//! # fn main() -> Result<()> {
//! # let streams = StreamSync::open(path, key)?;
//! let id = streams.create()?;
//! // dht.provide(id)
//! let stream = streams.append(&id)?;
//! stream.write_all(b"hello world")?;
//! let head = stream.commit()?;
//! // gossip.publish(id, head)
//! // dht.put_record(id, head)
//! # Ok(()) }
//! ```
//!
//! ## Replicated streams
//!
//! ```ignore
//! # use anyhow::Result;
//! # fn main() -> Result<()> {
//! # let streams = StreamSync::open(path, key)?;
//! # let id = StreamId::new([0; 32], 0);
//! streams.subscribe(id)?;
//! // let providers = dht.providers(id)
//! # let providers = &[];
//! streams.add_peers(id, providers);
//! // let head = dht.get_record(id)
//! streams.update_head(head);
//! // let heads = gossip.subscribe(id)
//! # let heads = futures::stream::empty();
//! while let Some(head) = heads.next().await {
//!     streams.update_head(head);
//! }
//! // dht.provide(id)
//! # Ok(()) }
//! ```
//!
//! ## Startup
//!
//! ```ignore
//! # use anyhow::Result;
//! # fn main() -> Result<()> {
//! # let streams = StreamSync::open(path, key)?;
//! for id in streams.streams() {
//!     // dht.provide(id)
//!     // gossip.subscribe(id)
//!     // let providers = dht.providers(id)
//!     # let providers = &[];
//!     streams.add_peers(id, providers);
//!     // let head = dht.get_record(id)
//!     streams.update_head(head);
//! }
//! # Ok(()) }
//! ```
use anyhow::Result;
use async_trait::async_trait;
use blake_streams_core::{Slice, SliceBuffer, StreamStorage, StreamWriter};
use fnv::FnvHashMap;
use futures::prelude::*;
use libp2p::core::connection::{ConnectedPoint, ConnectionId, ListenerId};
use libp2p::request_response::{
    ProtocolName, ProtocolSupport, RequestId, RequestResponse, RequestResponseCodec,
    RequestResponseConfig, RequestResponseEvent, RequestResponseMessage, ResponseChannel,
};
use libp2p::swarm::{
    IntoProtocolsHandler, NetworkBehaviour, NetworkBehaviourAction, PollParameters,
    ProtocolsHandler,
};
use libp2p::{Multiaddr, PeerId};
use std::collections::VecDeque;
use std::convert::TryInto;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use zerocopy::{AsBytes, FromBytes, LayoutVerified};

pub use blake_streams_core::{
    Head, Keypair, PublicKey, SecretKey, SignedHead, StreamId, StreamReader,
};
pub type Channel = ResponseChannel<Slice>;

pub struct LocalStreamWriter {
    inner: StreamWriter<Arc<Keypair>>,
}

impl LocalStreamWriter {
    pub fn head(&self) -> &Head {
        self.inner.head().head()
    }

    /// The signed head should be inserted as a record in the dht
    /// and published via gossipsub.
    pub fn commit(&mut self) -> io::Result<SignedHead> {
        self.inner.commit()
    }
}

impl Write for LocalStreamWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[derive(Clone, Debug)]
pub struct StreamSyncProtocol;

impl ProtocolName for StreamSyncProtocol {
    fn protocol_name(&self) -> &'static [u8] {
        b"/ipfs-embed/stream-sync/1.0.0"
    }
}

#[derive(Clone, Debug, Default)]
pub struct StreamSyncCodec {
    buffer: Vec<u8>,
}

#[async_trait]
impl RequestResponseCodec for StreamSyncCodec {
    type Protocol = StreamSyncProtocol;
    type Request = StreamSyncRequest;
    type Response = Slice;

    async fn read_request<T>(&mut self, _: &Self::Protocol, io: &mut T) -> io::Result<Self::Request>
    where
        T: AsyncRead + Send + Unpin,
    {
        self.buffer.clear();
        // unsafe { self.buffer.set_len(std::mem::size_of::<StreamSyncRequest>()); }
        self.buffer
            .resize(std::mem::size_of::<StreamSyncRequest>(), 0);
        io.read_exact(&mut self.buffer).await?;
        let layout = LayoutVerified::<_, StreamSyncRequest>::new(&self.buffer[..]).unwrap();
        let req = StreamSyncRequest {
            stream: layout.stream,
            start: layout.start,
            len: layout.len,
        };
        tracing::trace!("read_request {:?}", req);
        Ok(req)
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Send + Unpin,
    {
        self.buffer.clear();
        // unsafe { self.buffer.set_len(std::mem::size_of::<SignedHead>()); }
        self.buffer.resize(std::mem::size_of::<SignedHead>(), 0);
        io.read_exact(&mut self.buffer).await?;
        let layout = LayoutVerified::<_, SignedHead>::new(&self.buffer[..]).unwrap();
        let mut slice = Slice::default();
        slice.head = SignedHead {
            head: layout.head,
            sig: layout.sig,
        };
        tracing::trace!("read_response {:?}", slice.head);
        self.buffer.clear();
        // TODO: make sure to limit this to a reasonable value
        io.read_to_end(&mut self.buffer).await?;
        slice.data = self.buffer.clone();
        Ok(slice)
    }

    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Send + Unpin,
    {
        tracing::trace!("write_request {:?}", req);
        io.write_all(req.as_bytes()).await?;
        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Send + Unpin,
    {
        tracing::trace!("write_response {:?}", res.head);
        io.write_all(res.head.as_bytes()).await?;
        io.write_all(&res.data).await?;
        Ok(())
    }
}

#[derive(AsBytes, FromBytes, Debug)]
#[repr(C)]
pub struct StreamSyncRequest {
    stream: StreamId,
    start: u64,
    len: u64,
}

#[derive(Debug)]
pub enum StreamSyncEvent {
    NewHead(Head),
}

struct ReplicatedStream {
    buffer: SliceBuffer,
    peers: Vec<PeerId>,
    head: SignedHead,
    sync: Option<SignedHead>,
}

impl ReplicatedStream {
    fn new(buffer: SliceBuffer) -> Self {
        let head = *buffer.head();
        Self {
            buffer,
            peers: Default::default(),
            head,
            sync: None,
        }
    }
}

pub struct StreamSync {
    inner: RequestResponse<StreamSyncCodec>,
    store: StreamStorage,
    streams: FnvHashMap<StreamId, ReplicatedStream>,
    requests: FnvHashMap<RequestId, RequestState>,
    events: VecDeque<StreamSyncEvent>,
    slice_len: usize,
    slice: Slice,
}

fn peer_id_xor(id: &PeerId, x: [u8; 32]) -> [u8; 32] {
    let mh: &libp2p::multihash::Multihash = id.as_ref();
    let bytes = &mh.digest()[4..];
    assert_eq!(bytes.len(), 32);
    let mut peer_id: [u8; 32] = bytes.try_into().unwrap();
    for (n, x) in peer_id.iter_mut().zip(x.iter()) {
        *n ^= x;
    }
    peer_id
}

struct RequestState {
    stream: StreamId,
    slice_index: usize,
    peer_index: usize,
}

#[derive(Debug)]
pub struct StreamSyncConfig {
    pub path: PathBuf,
    pub key: Keypair,
    pub slice_len: usize,
    pub request_timeout: Duration,
    pub connection_keep_alive: Duration,
}

impl StreamSyncConfig {
    pub fn new(path: PathBuf, key: Keypair) -> Self {
        Self {
            path,
            key,
            slice_len: 65536,
            request_timeout: Duration::from_secs(10),
            connection_keep_alive: Duration::from_secs(10),
        }
    }
}

impl StreamSync {
    pub fn new(config: StreamSyncConfig) -> Result<Self> {
        let codec = StreamSyncCodec::default();
        let protocols = std::iter::once((StreamSyncProtocol, ProtocolSupport::Full));
        let mut cfg = RequestResponseConfig::default();
        cfg.set_request_timeout(config.request_timeout);
        cfg.set_connection_keep_alive(config.connection_keep_alive);
        let inner = RequestResponse::new(codec, protocols, cfg);
        let store = StreamStorage::open(&config.path, config.key)?;
        let slice = Slice::with_capacity(config.slice_len);
        Ok(Self {
            inner,
            store,
            streams: Default::default(),
            events: Default::default(),
            slice,
            slice_len: config.slice_len,
            requests: Default::default(),
        })
    }

    pub fn streams(&mut self) -> Result<Vec<StreamId>> {
        self.store.streams().collect()
    }

    pub fn head(&mut self, id: &StreamId) -> Result<Option<Head>> {
        self.store.head(id)
    }

    pub fn slice(&mut self, id: &StreamId, start: u64, len: u64) -> Result<StreamReader> {
        self.store.slice(id, start, len)
    }

    pub fn remove(&mut self, id: &StreamId) -> Result<()> {
        self.store.remove(id)
    }

    pub fn append(&mut self, id: u64) -> Result<LocalStreamWriter> {
        Ok(LocalStreamWriter {
            inner: self.store.append(id)?,
        })
    }

    pub fn subscribe(&mut self, id: &StreamId) -> Result<()> {
        let stream = self.store.subscribe(id)?;
        let stream = SliceBuffer::new(stream, self.slice_len as u64);
        self.streams.insert(*id, ReplicatedStream::new(stream));
        Ok(())
    }

    pub fn add_peers(&mut self, id: &StreamId, peers: impl Iterator<Item = PeerId>) {
        // creates a deterministic unique order for each peer
        // by sorting the xored public keys.
        let key = self.store.public_key().to_bytes();
        if let Some(stream) = self.streams.get_mut(id) {
            stream.peers.extend(peers);
            stream
                .peers
                .sort_unstable_by(|a, b| peer_id_xor(a, key).cmp(&peer_id_xor(b, key)));
            stream.peers.dedup();
            tracing::info!("{} has {} peers", id, stream.peers.len());
        } else {
            tracing::info!("add_peers: missing stream {}", id);
        }
    }

    pub fn remove_peer(&mut self, id: &StreamId, peer: &PeerId) {
        if let Some(stream) = self.streams.get_mut(id) {
            stream.peers.retain(|p| p != peer);
        }
    }

    pub fn update_head(&mut self, head: SignedHead) {
        let stream = if let Some(stream) = self.streams.get_mut(head.head().id()) {
            stream
        } else {
            tracing::error!("no stream");
            return;
        };
        if head.head().len() <= stream.head.head().len() {
            tracing::info!("duplicate head");
            return;
        }
        if let Err(err) = head.verify(stream.buffer.id()) {
            tracing::error!("invalid head: {}", err);
            return;
        }
        stream.head = head;
        if stream.sync.is_none() && !stream.peers.is_empty() {
            self.start_sync(head);
        }
    }

    fn start_sync(&mut self, head: SignedHead) {
        tracing::info!(
            "starting sync {} to {}",
            head.head().id(),
            head.head().len()
        );
        let stream = if let Some(stream) = self.streams.get_mut(head.head().id()) {
            stream
        } else {
            return;
        };
        stream.sync = Some(head);
        stream
            .buffer
            .prepare(head.head().len() - stream.buffer.head().head().len());
        for i in 0..stream.buffer.slices().len() {
            let request = RequestState {
                stream: *head.head().id(),
                slice_index: i,
                peer_index: 0,
            };
            self.request_slice(request);
        }
    }

    fn request_slice(&mut self, state: RequestState) {
        let stream = self.streams.get(&state.stream).unwrap();
        let peer = stream.peers[(state.slice_index + state.peer_index) % stream.peers.len()];
        let slice = &stream.buffer.slices()[state.slice_index];
        let request = StreamSyncRequest {
            stream: state.stream,
            start: slice.offset,
            len: slice.len,
        };
        let req = self.inner.send_request(&peer, request);
        self.requests.insert(req, state);
    }

    fn send_slice(&mut self, ch: Channel, id: &StreamId, start: u64, len: u64) -> Result<()> {
        self.store.extract(id, start, len, &mut self.slice)?;
        self.inner.send_response(ch, self.slice.clone()).ok();
        Ok(())
    }

    fn recv_slice(&mut self, req: RequestId, slice: Slice) {
        let stream = if let Some(stream) = self.streams.get_mut(slice.head.head().id()) {
            stream
        } else {
            return;
        };
        let mut req = if let Some(req) = self.requests.remove(&req) {
            req
        } else {
            return;
        };
        if let Err(err) = stream.buffer.add_slice(&slice, req.slice_index) {
            tracing::info!("error adding slice: {}", err);
            req.peer_index += 1;
            self.request_slice(req);
            return;
        }
        if stream.buffer.commitable() {
            let head = stream.sync.take().unwrap();
            // TODO don't unwrap
            let head = stream.buffer.commit(*head.sig()).unwrap();
            self.events.push_back(StreamSyncEvent::NewHead(head));
            let head = stream.head;
            if head.head().len() > stream.buffer.head().head().len() {
                self.start_sync(head);
            }
        }
    }

    fn recv_error(&mut self, req: RequestId) {
        if let Some(mut req) = self.requests.remove(&req) {
            req.peer_index += 1;
            self.request_slice(req);
        }
    }
}

impl NetworkBehaviour for StreamSync {
    type ProtocolsHandler =
        <RequestResponse<StreamSyncCodec> as NetworkBehaviour>::ProtocolsHandler;
    type OutEvent = StreamSyncEvent;

    fn new_handler(&mut self) -> <Self as NetworkBehaviour>::ProtocolsHandler {
        self.inner.new_handler()
    }

    fn addresses_of_peer(&mut self, peer: &PeerId) -> Vec<Multiaddr> {
        self.inner.addresses_of_peer(peer)
    }

    fn inject_connected(&mut self, peer: &PeerId) {
        self.inner.inject_connected(peer)
    }

    fn inject_disconnected(&mut self, peer: &PeerId) {
        self.inner.inject_disconnected(peer)
    }

    fn inject_connection_established(
        &mut self,
        peer: &PeerId,
        conn: &ConnectionId,
        cp: &ConnectedPoint,
    ) {
        self.inner.inject_connection_established(peer, conn, cp)
    }

    fn inject_connection_closed(
        &mut self,
        peer: &PeerId,
        conn: &ConnectionId,
        cp: &ConnectedPoint,
    ) {
        self.inner.inject_connection_closed(peer, conn, cp)
    }

    fn inject_addr_reach_failure(
        &mut self,
        peer_id: Option<&PeerId>,
        addr: &Multiaddr,
        error: &dyn std::error::Error,
    ) {
        self.inner.inject_addr_reach_failure(peer_id, addr, error)
    }

    fn inject_dial_failure(&mut self, peer_id: &PeerId) {
        self.inner.inject_dial_failure(peer_id)
    }

    fn inject_new_listen_addr(&mut self, id: ListenerId, addr: &Multiaddr) {
        self.inner.inject_new_listen_addr(id, addr)
    }

    fn inject_expired_listen_addr(&mut self, id: ListenerId, addr: &Multiaddr) {
        self.inner.inject_expired_listen_addr(id, addr)
    }

    fn inject_new_external_addr(&mut self, addr: &Multiaddr) {
        self.inner.inject_new_external_addr(addr)
    }

    fn inject_expired_external_addr(&mut self, addr: &Multiaddr) {
        self.inner.inject_expired_external_addr(addr)
    }

    fn inject_listener_error(&mut self, id: ListenerId, err: &(dyn std::error::Error + 'static)) {
        self.inner.inject_listener_error(id, err)
    }

    fn inject_listener_closed(&mut self, id: ListenerId, reason: Result<(), &std::io::Error>) {
        self.inner.inject_listener_closed(id, reason)
    }

    fn inject_event(
        &mut self,
        peer: PeerId,
        conn: ConnectionId,
        event: <<Self::ProtocolsHandler as IntoProtocolsHandler>::Handler as ProtocolsHandler>::OutEvent,
    ) {
        self.inner.inject_event(peer, conn, event)
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
        pp: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction<<<Self::ProtocolsHandler as IntoProtocolsHandler>::Handler as ProtocolsHandler>::InEvent, Self::OutEvent>>
    {
        loop {
            if let Some(event) = self.events.pop_front() {
                return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
            }
            let action = match self.inner.poll(cx, pp) {
                Poll::Ready(action) => action,
                Poll::Pending => return Poll::Pending,
            };
            let event = match action {
                NetworkBehaviourAction::GenerateEvent(event) => event,
                NetworkBehaviourAction::DialAddress { address } => {
                    return Poll::Ready(NetworkBehaviourAction::DialAddress { address });
                }
                NetworkBehaviourAction::DialPeer { peer_id, condition } => {
                    return Poll::Ready(NetworkBehaviourAction::DialPeer { peer_id, condition });
                }
                NetworkBehaviourAction::NotifyHandler {
                    peer_id,
                    handler,
                    event,
                } => {
                    return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
                        peer_id,
                        handler,
                        event,
                    });
                }
                NetworkBehaviourAction::ReportObservedAddr { address, score } => {
                    return Poll::Ready(NetworkBehaviourAction::ReportObservedAddr {
                        address,
                        score,
                    });
                }
                NetworkBehaviourAction::CloseConnection {
                    connection,
                    peer_id,
                } => {
                    return Poll::Ready(NetworkBehaviourAction::CloseConnection {
                        connection,
                        peer_id,
                    });
                }
            };
            match event {
                RequestResponseEvent::Message { peer: _, message } => match message {
                    RequestResponseMessage::Request {
                        request_id: _,
                        request,
                        channel,
                    } => {
                        if let Err(err) =
                            self.send_slice(channel, &request.stream, request.start, request.len)
                        {
                            tracing::info!("error sending slice: {}", err);
                        }
                    }
                    RequestResponseMessage::Response {
                        request_id,
                        response,
                    } => {
                        self.recv_slice(request_id, response);
                    }
                },
                RequestResponseEvent::ResponseSent {
                    peer: _,
                    request_id: _,
                } => {}
                RequestResponseEvent::OutboundFailure {
                    peer: _,
                    request_id,
                    error,
                } => {
                    tracing::error!("outbound failure: {}", error);
                    self.recv_error(request_id);
                }
                RequestResponseEvent::InboundFailure {
                    peer: _,
                    request_id: _,
                    error,
                } => {
                    tracing::error!("inbound failure: {}", error);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use libp2p::core::muxing::StreamMuxerBox;
    use libp2p::core::transport::{Boxed, MemoryTransport, Transport};
    use libp2p::core::upgrade::Version;
    use libp2p::identity;
    use libp2p::plaintext::PlainText2Config;
    use libp2p::swarm::{Swarm, SwarmEvent};
    use libp2p::yamux::YamuxConfig;
    use rand::RngCore;
    use std::io::Read;
    use std::time::{Duration, Instant};
    use tempdir::TempDir;

    fn tracing_try_init() {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init()
            .ok();
    }

    fn rand_bytes(size: usize) -> Vec<u8> {
        let mut rng = rand::thread_rng();
        let mut data = Vec::with_capacity(size);
        data.resize(data.capacity(), 0);
        rng.fill_bytes(&mut data);
        data
    }

    fn keypair(secret: [u8; 32]) -> Keypair {
        let secret = SecretKey::from_bytes(&secret).unwrap();
        let public = PublicKey::from(&secret);
        Keypair { secret, public }
    }

    fn build_dev_transport(
        key_pair: identity::Keypair,
    ) -> anyhow::Result<Boxed<(PeerId, StreamMuxerBox)>> {
        let plaintext_config = PlainText2Config {
            local_public_key: key_pair.public(),
        };
        let yamux_config = YamuxConfig::default();
        let transport = MemoryTransport {}
            .upgrade(Version::V1)
            .authenticate(plaintext_config)
            .multiplex(yamux_config)
            .timeout(Duration::from_secs(10))
            .map(|(peer_id, muxer), _| (peer_id, StreamMuxerBox::new(muxer)))
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))
            .boxed();
        Ok(transport)
    }

    fn build_swarm(
        path: PathBuf,
        mut secret: [u8; 32],
        slice_len: usize,
    ) -> Result<Swarm<StreamSync>> {
        let mut config = StreamSyncConfig::new(path, keypair(secret));
        config.slice_len = slice_len;
        let behaviour = StreamSync::new(config)?;
        let secret = identity::ed25519::SecretKey::from_bytes(&mut secret)?;
        let key = identity::Keypair::Ed25519(secret.into());
        let peer_id = key.public().into_peer_id();
        let transport = build_dev_transport(key)?;
        Ok(Swarm::new(transport, behaviour, peer_id))
    }

    #[async_std::test]
    async fn test_sync() -> Result<()> {
        tracing_try_init();
        let tmp = TempDir::new("libp2p_blake_streams")?;
        let mut server = build_swarm(tmp.path().join("server"), [0; 32], 65536)?;
        server.listen_on("/memory/1".parse()?)?;
        let mut client = build_swarm(tmp.path().join("client"), [1; 32], 65536)?;

        let data = rand_bytes(1024 * 1024);
        let mut stream = server.behaviour_mut().append(0)?;
        stream.write_all(&data)?;
        let head = stream.commit()?;

        client.behaviour_mut().subscribe(head.head().id())?;
        client
            .behaviour_mut()
            .set_peers(head.head().id(), vec![*server.local_peer_id()]);
        client.dial_addr("/memory/1".parse().unwrap())?;

        let mut start = None;

        loop {
            futures::select! {
                ev = server.next_event().fuse() => {
                    tracing::info!("server: {:?}", ev);
                }
                ev = client.next_event().fuse() => {
                    tracing::info!("client: {:?}", ev);
                    match ev {
                        SwarmEvent::ConnectionEstablished { .. } => {
                            client.behaviour_mut().update_head(head);
                            start = Some(Instant::now());
                        }
                        SwarmEvent::Behaviour(StreamSyncEvent::NewHead(_)) => {
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }

        let time = start.unwrap().elapsed();
        println!(
            "synced {} in {}ms ({:.2} MB/s)",
            data.len(),
            time.as_millis(),
            data.len() as f64 / time.as_micros() as f64
        );

        let mut data2 = vec![];
        let mut stream = client
            .behaviour_mut()
            .slice(head.head().id(), 0, head.head().len())?;
        stream.read_to_end(&mut data2)?;
        assert_eq!(data, data2);

        Ok(())
    }
}
