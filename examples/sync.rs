use anyhow::Result;
use blake_streams::{Slice, SliceBuffer, StreamStorage};
use ed25519_dalek::{Keypair, PublicKey, SecretKey};
use rand::RngCore;
use std::io::Write;
use tempdir::TempDir;

fn rand_bytes(size: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let mut data = vec![0; size];
    rng.fill_bytes(&mut data);
    data
}

fn keypair(secret: [u8; 32]) -> Keypair {
    let secret = SecretKey::from_bytes(&secret).unwrap();
    let public = PublicKey::from(&secret);
    Keypair { secret, public }
}

fn main() -> Result<()> {
    let len = 1024 * 1024 * 1024;
    let slice_len = 8192;
    let tmp = TempDir::new("sync")?;
    let data = rand_bytes(len as usize);
    let path = tmp.path().join("server");
    let server = StreamStorage::open(&path, keypair([0; 32]))?;
    let id = server.create_local_stream()?;
    let mut stream = server.append_local_stream(&id)?;
    stream.write_all(&data)?;
    stream.flush()?;
    stream.commit()?;

    let path = tmp.path().join("client");
    let client = StreamStorage::open(&path, keypair([1; 32]))?;
    client.create_replicated_stream(&id)?;
    let stream = client.append_replicated_stream(&id)?;
    let mut buffer = SliceBuffer::new(stream, slice_len);

    let mut slice = Slice::default();
    buffer.prepare(len);
    for i in 0..buffer.slices().len() {
        let info = &buffer.slices()[i];
        server.extract(&id, info.offset, info.len, &mut slice)?;
        buffer.add_slice(&slice, i)?;
    }
    buffer.commit(*slice.head.sig())?;

    Ok(())
}
