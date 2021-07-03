use anyhow::Result;
use futures::channel::oneshot;
use futures::prelude::*;
use ipfs_embed::{Event, Key, LocalStreamWriter, Quorum, Record, SignedHead};
use std::io::{self, Write};
use std::pin::Pin;
use std::task::{Context, Poll};
use zerocopy::{AsBytes, LayoutVerified};

pub type Ipfs = ipfs_embed::Ipfs<ipfs_embed::DefaultParams>;
pub use ipfs_embed::{self, Head, StreamId, StreamReader, SwarmEvents};

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

    pub fn streams(&self) -> Result<Vec<StreamId>> {
        self.ipfs.streams()
    }

    pub fn head(&self, id: &StreamId) -> Result<Option<Head>> {
        self.ipfs.stream_head(id)
    }

    pub fn slice(&self, id: &StreamId, start: u64, len: u64) -> Result<StreamReader> {
        self.ipfs.stream_slice(id, start, len)
    }

    pub fn remove(&self, id: &StreamId) -> Result<()> {
        self.ipfs.stream_remove(id)
    }

    pub async fn append(&self, id: u64) -> Result<StreamWriter> {
        let writer = self.ipfs.stream_append(id)?;
        self.ipfs.provide(writer.head().id().key()).await?;
        Ok(StreamWriter {
            inner: writer,
            ipfs: self.ipfs.clone(),
        })
    }

    pub async fn subscribe(&self, id: &StreamId) -> Result<BlakeStream> {
        let dht_key = id.key();
        let events = self.ipfs.swarm_events();
        self.ipfs.stream_subscribe(id)?;
        let peers = self.ipfs.providers(dht_key.clone()).await?;
        // TODO: also add peers that subscribe to id via gossip
        tracing::info!("found {} peers", peers.len());
        self.ipfs.stream_set_peers(id, peers.into_iter().collect());
        let mut stream = self.ipfs.subscribe(&id.topic())?;
        let records = self
            .ipfs
            .get_record(&dht_key, Quorum::One)
            .await
            .ok()
            .unwrap_or_default();
        for record in records {
            if let Some(head) = LayoutVerified::<_, SignedHead>::new(&record.record.value[..]) {
                tracing::info!("new head from dht {}", head.head().len());
                self.ipfs.stream_update_head(*head);
            }
        }
        let ipfs = self.ipfs.clone();
        self.ipfs.provide(dht_key).await?;
        let (exit_tx, mut exit_rx) = oneshot::channel();
        async_global_executor::spawn(async move {
            let exit = &mut exit_rx;
            loop {
                futures::select! {
                    _ = exit.fuse() => break,
                    head = stream.next().fuse() => {
                        if let Some(head) = head {
                            match LayoutVerified::<_, SignedHead>::new(&head[..]) {
                                Some(head) => {
                                    tracing::info!("new head from gossip {}", head.head().len());
                                    ipfs.stream_update_head(*head);
                                }
                                None => tracing::debug!("failed to decode head"),
                            }
                        }
                    }
                }
            }
        })
        .detach();
        Ok(BlakeStream {
            events,
            exit: Some(exit_tx),
        })
    }
}

pub struct BlakeStream {
    events: SwarmEvents,
    exit: Option<oneshot::Sender<()>>,
}

impl Stream for BlakeStream {
    type Item = Head;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        loop {
            return match Pin::new(&mut self.events).poll_next(cx) {
                Poll::Ready(Some(Event::NewHead(head))) => {
                    tracing::info!("sync complete {}", head.len());
                    Poll::Ready(Some(head))
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
                _ => continue,
            };
        }
    }
}

impl Drop for BlakeStream {
    fn drop(&mut self) {
        self.exit.take().unwrap().send(()).ok();
    }
}

trait StreamIdExt {
    fn key(&self) -> Key;
    fn topic(&self) -> String;
}

impl StreamIdExt for StreamId {
    fn key(&self) -> Key {
        Key::new(&self.as_bytes())
    }

    fn topic(&self) -> String {
        self.to_string()
    }
}

pub struct StreamWriter {
    inner: LocalStreamWriter,
    ipfs: Ipfs,
}

impl StreamWriter {
    pub fn id(&self) -> &StreamId {
        self.inner.head().id()
    }

    pub fn head(&self) -> &Head {
        self.inner.head()
    }

    pub async fn commit(&mut self) -> Result<()> {
        let head = self.inner.commit()?;
        tracing::info!("publishing head {}", head.head().len());
        self.ipfs
            .publish(&self.id().topic(), head.as_bytes().to_vec())?;
        let record = Record::new(self.id().key(), head.as_bytes().to_vec());
        self.ipfs.put_record(record, Quorum::One).await?;
        Ok(())
    }
}

impl Write for StreamWriter {
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

        let bootstrap = create_swarm(tmp.path().join("bootstrap")).await?;
        let addr = bootstrap.ipfs().listeners()[0].clone();
        let peer_id = bootstrap.ipfs().local_peer_id();
        let nodes = [(peer_id, addr)];

        let server = create_swarm(tmp.path().join("server")).await?;
        server.ipfs().bootstrap(&nodes).await?;
        let client = create_swarm(tmp.path().join("client")).await?;
        client.ipfs().bootstrap(&nodes).await?;

        let mut append = server.append(0).await?;
        let id = *append.id();
        append.write_all(&first)?;
        append.commit().await?;

        let mut stream = client.subscribe(&id).await?;

        let head = stream.next().await.unwrap();
        let mut buf = vec![];
        client
            .slice(head.id(), 0, head.len())?
            .read_to_end(&mut buf)?;
        assert_eq!(buf, first);

        append.write_all(&second)?;
        append.commit().await?;

        let head2 = stream.next().await.unwrap();
        let mut buf = vec![];
        client
            .slice(head2.id(), head.len(), head2.len() - head.len())?
            .read_to_end(&mut buf)?;
        assert_eq!(buf, second);

        Ok(())
    }
}
