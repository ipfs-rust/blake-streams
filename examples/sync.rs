use anyhow::Result;
use blake_streams::{SliceBuffer, StreamStorage};
use rand::RngCore;
use std::io::Write;
use tempdir::TempDir;

fn rand_bytes(size: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let mut data = vec![0; size];
    rng.fill_bytes(&mut data);
    data
}

fn main() -> Result<()> {
    let len = 1024 * 1024 * 1024;
    let slice_len = 8192;
    let tmp = TempDir::new("sync")?;
    let data = rand_bytes(len as usize);
    let path = tmp.path().join("server");
    let server = StreamStorage::open(&path)?;
    let server_sid = server.create_stream()?;
    let mut stream = server.append(server_sid)?;
    stream.write_all(&data)?;
    stream.flush()?;
    let hash = stream.commit()?;

    let path = tmp.path().join("client");
    let client = StreamStorage::open(&path)?;
    let client_sid = client.create_stream()?;
    let stream = client.append(client_sid)?;
    let mut buffer = SliceBuffer::new(stream, hash, slice_len);

    let mut slice = Vec::with_capacity(slice_len as usize * 2);
    buffer.prepare(len);
    for i in 0..buffer.slices().len() {
        let info = &buffer.slices()[i];
        server.extract(server_sid, info.offset, info.len, &mut slice)?;
        buffer.add_slice(&slice, i)?;
        slice.clear();
    }
    buffer.commit()?;

    Ok(())
}
