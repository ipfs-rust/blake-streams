use anyhow::Result;
use blake_streams_core::{DocId, Slice, SliceBuffer, StreamStorage};
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
    let mut server = StreamStorage::open(&path, keypair([0; 32]))?;
    let mut stream = server.append(DocId::unique())?;
    stream.write_all(&data)?;
    stream.flush()?;
    let head = stream.commit()?;

    let path = tmp.path().join("client");
    let mut client = StreamStorage::open(&path, keypair([1; 32]))?;
    let stream = client.subscribe(stream.id())?;
    let mut stream = SliceBuffer::new(stream, slice_len);

    let mut slice = Slice::default();
    stream.prepare(len);
    for i in 0..stream.slices().len() {
        let info = &stream.slices()[i];
        server.extract(stream.id(), info.offset, info.len, &mut slice)?;
        stream.add_slice(&slice, i)?;
    }
    stream.commit(*head.sig())?;

    Ok(())
}
