//! # Blake streams
//!
//! ## Local streams
//!
//! ```rust
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
//! ```rust
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
//! ```rust
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
use blake_streams::{
    Head, Keypair, SignedHead, Slice, SliceBuffer, StreamId, StreamReader, StreamStorage,
    StreamWriter,
};
use fnv::{FnvHashMap, FnvHashSet};
use libp2p::core::connection::{ConnectedPoint, ConnectionId};
use libp2p::streaming_response::{
    ChannelId, Codec, StreamingResponse, StreamingResponseConfig, StreamingResponseEvent,
};
use libp2p::swarm::{
    IntoProtocolsHandler, NetworkBehaviour, NetworkBehaviourAction, PollParameters,
    ProtocolsHandler,
};
use libp2p::{Multiaddr, PeerId};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;
use std::task::{Context, Poll};

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
pub struct StreamSyncCodec;

impl Codec for StreamSyncCodec {
    type Request = StreamSyncRequest;
    type Response = Slice;

    fn protocol_info() -> &'static [u8] {
        b"/ipfs-embed/stream-sync/1.0.0"
    }
}

#[derive(Deserialize, Serialize)]
pub struct StreamSyncRequest {
    stream: StreamId,
    start: u64,
    len: u64,
}

pub enum StreamSyncEvent {
    NewHead(Head),
}

struct ReplicatedStream {
    buffer: SliceBuffer,
    peers: FnvHashSet<PeerId>,
    head: Option<SignedHead>,
}

impl ReplicatedStream {
    fn new(buffer: SliceBuffer) -> Self {
        Self {
            buffer,
            peers: Default::default(),
            head: None,
        }
    }
}

pub struct StreamSync {
    inner: StreamingResponse<StreamSyncCodec>,
    store: StreamStorage,
    streams: FnvHashMap<StreamId, ReplicatedStream>,
    events: VecDeque<StreamSyncEvent>,
    slice_len: usize,
    slice: Slice,
}

impl StreamSync {
    pub fn open(path: &Path, key: Keypair, slice_len: usize) -> Result<Self> {
        let inner = StreamingResponse::new(StreamingResponseConfig::default());
        let store = StreamStorage::open(path, key)?;
        let slice = Slice::with_capacity(slice_len);
        Ok(Self {
            inner,
            store,
            streams: Default::default(),
            events: Default::default(),
            slice,
            slice_len,
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

    pub fn add_peers(&mut self, id: &StreamId, peers: &[PeerId]) {
        if let Some(stream) = self.streams.get_mut(id) {
            for peer in peers {
                stream.peers.insert(*peer);
            }
        }
    }

    pub fn update_head(&mut self, head: SignedHead) {
        let stream = if let Some(stream) = self.streams.get_mut(head.head().id()) {
            stream
        } else {
            return;
        };
        if let Err(err) = head.verify(stream.buffer.id()) {
            tracing::error!("invalid head: {}", err);
            return;
        }
        if head.head().len() <= stream.buffer.head().len() {
            return;
        }
        if let Some(chead) = stream.head.as_ref() {
            if head.head().len() <= chead.head().len() {
                return;
            }
        }
        stream.head = Some(head);
        // TODO send requests
    }

    fn send_slice(&mut self, ch: ChannelId, id: &StreamId, start: u64, len: u64) -> Result<()> {
        self.store.extract(id, start, len, &mut self.slice)?;
        self.inner.respond_final(ch, self.slice.clone())?;
        Ok(())
    }
}

impl NetworkBehaviour for StreamSync {
    type ProtocolsHandler =
        <StreamingResponse<StreamSyncCodec> as NetworkBehaviour>::ProtocolsHandler;
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

    fn inject_connection_closed(
        &mut self,
        peer: &PeerId,
        conn: &ConnectionId,
        cp: &ConnectedPoint,
    ) {
        self.inner.inject_connection_closed(peer, conn, cp)
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
            };
            match event {
                StreamingResponseEvent::ReceivedRequest {
                    channel_id,
                    payload,
                } => {
                    if let Err(err) =
                        self.send_slice(channel_id, &payload.stream, payload.start, payload.len)
                    {
                        tracing::info!("error sending slice: {}", err);
                    }
                }
                StreamingResponseEvent::CancelledRequest {
                    channel_id: _,
                    reason: _,
                } => {}
                StreamingResponseEvent::ResponseReceived {
                    request_id,
                    sequence_no,
                    payload,
                } => {
                    // TODO
                }
                StreamingResponseEvent::ResponseFinished {
                    request_id: _,
                    sequence_no: _,
                } => {
                    // TODO retry
                }
            }
        }
    }
}
