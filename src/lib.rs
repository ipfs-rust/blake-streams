use anyhow::Result;
use futures::channel::mpsc;
use futures::prelude::*;
use ipfs_embed::{Event, GossipEvent, LocalStreamWriter, SignedHead};
use std::io::{self, Write};
use std::pin::Pin;
use std::task::{Context, Poll};
use zerocopy::{AsBytes, LayoutVerified};

pub type Ipfs = ipfs_embed::Ipfs<ipfs_embed::DefaultParams>;
pub use blake_streams_core::PeerId;
pub use ipfs_embed::{self, DocId, Head, StreamId, StreamReader, SwarmEvents};

pub struct BlakeStreams {
    ipfs: Ipfs,
}

impl BlakeStreams {
    pub fn new(ipfs: Ipfs) -> Self {
        Self { ipfs }
    }

    pub fn ipfs(&self) -> &Ipfs {
        &self.ipfs
    }

    pub fn streams(&self, doc: DocId) -> Result<Vec<StreamId>> {
        self.ipfs.substreams(doc)
    }

    pub fn head(&self, id: &StreamId) -> Result<Option<SignedHead>> {
        self.ipfs.stream_head(id)
    }

    pub fn slice(&self, id: &StreamId, start: u64, len: u64) -> Result<StreamReader> {
        self.ipfs.stream_slice(id, start, len)
    }

    pub fn remove(&self, id: &StreamId) -> Result<()> {
        self.ipfs.stream_remove(id)
    }

    pub fn link_stream(&self, id: &StreamId) -> Result<()> {
        tracing::info!("{}: linking stream (peer {})", id.doc(), id.peer());
        self.ipfs.stream_subscribe(id)
    }

    pub async fn subscribe(&self, doc: DocId) -> Result<DocStream> {
        tracing::info!("{}: subscribe", doc);
        let events = self.ipfs.swarm_events();
        let (mut tx, rx) = mpsc::channel(1);
        let _ = tx.try_send(());
        let ipfs = self.ipfs.clone();
        async_global_executor::spawn(doc_task(
            doc,
            ipfs.swarm_events(),
            ipfs.subscribe(&doc.to_string())?,
            ipfs,
            tx.clone(),
            rx,
        ))
        .detach();
        Ok(DocStream {
            doc,
            ipfs: self.ipfs.clone(),
            events,
            publisher: tx,
        })
    }
}

async fn doc_task(
    doc: DocId,
    mut events: SwarmEvents,
    mut stream: impl Stream<Item = GossipEvent> + Send + Unpin + 'static,
    ipfs: Ipfs,
    mut tx: mpsc::Sender<()>,
    mut rx: mpsc::Receiver<()>,
) {
    let local_peer_id = ipfs.local_public_key().into();
    loop {
        futures::select! {
            ev = rx.next().fuse() => match ev {
                Some(()) => {
                    let head = match ipfs.stream_head(&StreamId::new(local_peer_id, doc)) {
                        Ok(Some(head)) => head,
                        Ok(None) => continue,
                        Err(err) => {
                            tracing::error!("fetching head err {}", err);
                            continue;
                        }
                    };
                    tracing::info!(
                        "{}: publish_head (peer {}) (offset {})",
                        doc,
                        head.head().id().peer(),
                        head.head().len(),
                    );
                    if let Err(err) = ipfs.publish(&doc.to_string(), head.as_bytes().to_vec()) {
                        tracing::info!("publish error: {}", err);
                    }
                }
                None => {
                    tracing::info!("exiting doc task");
                    return;
                }
            },
            ev = stream.next().fuse() => match ev {
                Some(GossipEvent::Message(_, head)) => {
                    match LayoutVerified::<_, SignedHead>::new(&head[..]) {
                        Some(head) => {
                            tracing::info!(
                                "{}: new_head (peer {}) (offset {})",
                                doc,
                                head.head().id().peer(),
                                head.head().len(),
                            );
                            ipfs.stream_update_head(*head);
                        }
                        None => tracing::debug!("gossip: failed to decode head"),
                    }
                }
                Some(GossipEvent::Subscribed(peer_id)) => {
                    tracing::info!("{}: new_peer (peer {})", doc, peer_id);
                    ipfs.stream_add_peers(doc, std::iter::once(peer_id));
                    let _ = tx.try_send(());
                }
                _ => {}
            },
            ev = events.next().fuse() => match ev {
                Some(Event::Discovered(peer_id)) => ipfs.dial(&peer_id),
                _ => {}
            }
        }
    }
}

pub struct DocStream {
    doc: DocId,
    ipfs: Ipfs,
    events: SwarmEvents,
    publisher: mpsc::Sender<()>,
}

impl Stream for DocStream {
    type Item = Head;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        loop {
            return match Pin::new(&mut self.events).poll_next(cx) {
                Poll::Ready(Some(Event::NewHead(head))) => {
                    if head.id().doc() != self.doc {
                        continue;
                    }
                    tracing::info!("{}: sync_complete (offset {})", self.doc, head.len());
                    Poll::Ready(Some(head))
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
                _ => continue,
            };
        }
    }
}

impl DocStream {
    pub fn append(&self) -> Result<DocWriter> {
        let writer = self.ipfs.stream_append(self.doc)?;
        Ok(DocWriter {
            inner: writer,
            publisher: self.publisher.clone(),
        })
    }
}

impl Drop for DocStream {
    fn drop(&mut self) {
        self.publisher.close_channel();
    }
}

pub struct DocWriter {
    inner: LocalStreamWriter,
    publisher: mpsc::Sender<()>,
}

impl DocWriter {
    pub fn id(&self) -> &StreamId {
        self.inner.head().id()
    }

    pub fn head(&self) -> &Head {
        self.inner.head()
    }

    pub fn commit(&mut self) -> Result<()> {
        self.inner.commit()?;
        let _ = self.publisher.try_send(());
        Ok(())
    }
}

impl Write for DocWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ipfs_embed::{generate_keypair, Config};
    use rand::RngCore;
    use std::io::Read;
    use std::path::PathBuf;
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

    async fn create_swarm(path: PathBuf) -> Result<BlakeStreams> {
        std::fs::create_dir_all(&path)?;
        let mut config = Config::new(&path, generate_keypair());
        config.network.broadcast = None;
        let ipfs = Ipfs::new(config).await?;
        ipfs.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?.next().await;
        Ok(BlakeStreams::new(ipfs))
    }

    #[async_std::test]
    async fn test_streams() -> anyhow::Result<()> {
        tracing_try_init();
        let tmp = TempDir::new("test_streams")?;

        let first = rand_bytes(8192);
        let second = rand_bytes(8192);

        let server = create_swarm(tmp.path().join("server")).await?;
        let client = create_swarm(tmp.path().join("client")).await?;

        let doc_id = DocId::unique();
        let doc = server.subscribe(doc_id).await?;
        let mut append = doc.append()?;
        append.write_all(&first)?;
        append.commit()?;

        let mut stream = client.subscribe(doc_id).await?;
        client.link_stream(append.id())?;

        let head = stream.next().await.unwrap();
        let mut buf = vec![];
        client
            .slice(head.id(), 0, head.len())?
            .read_to_end(&mut buf)?;
        assert_eq!(buf, first);

        append.write_all(&second)?;
        append.commit()?;

        let head2 = stream.next().await.unwrap();
        let mut buf = vec![];
        client
            .slice(head2.id(), head.len(), head2.len() - head.len())?
            .read_to_end(&mut buf)?;
        assert_eq!(buf, second);

        Ok(())
    }
}
