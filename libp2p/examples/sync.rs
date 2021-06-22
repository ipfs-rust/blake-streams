use anyhow::Result;
use blake_streams::{PublicKey, SecretKey};
use futures::stream::StreamExt;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::{Boxed, MemoryTransport, Transport};
use libp2p::core::upgrade::Version;
use libp2p::plaintext::PlainText2Config;
use libp2p::swarm::{Swarm, SwarmEvent};
use libp2p::yamux::YamuxConfig;
use libp2p::{identity, PeerId};
use libp2p_blake_streams::{Keypair, StreamSync, StreamSyncEvent};
use rand::RngCore;
use std::io::{self, Read, Write};
use std::path::Path;
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

fn build_swarm(path: &Path, mut secret: [u8; 32], slice_len: usize) -> Result<Swarm<StreamSync>> {
    let behaviour = StreamSync::open(path, keypair(secret), slice_len)?;
    let secret = identity::ed25519::SecretKey::from_bytes(&mut secret)?;
    let key = identity::Keypair::Ed25519(secret.into());
    let peer_id = key.public().into_peer_id();
    let transport = build_dev_transport(key)?;
    Ok(Swarm::new(transport, behaviour, peer_id))
}

#[async_std::main]
async fn main() -> Result<()> {
    tracing_try_init();
    let tmp = TempDir::new("libp2p_blake_streams")?;
    let mut server = build_swarm(&tmp.path().join("server"), [0; 32], 65536)?;
    server.listen_on("/memory/1".parse()?)?;
    let mut client = build_swarm(&tmp.path().join("client"), [1; 32], 65536)?;

    let data = rand_bytes(1024 * 1024 * 100);
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
            ev = server.next() => {
                tracing::info!("server: {:?}", ev.unwrap());
            }
            ev = client.next() => {
                let ev = ev.unwrap();
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
