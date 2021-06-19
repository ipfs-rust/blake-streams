use anyhow::Result;
use blake_streams::{SliceBuffer, StreamStorage};
use criterion::measurement::WallTime;
use criterion::{criterion_group, criterion_main, Bencher, BenchmarkId, Criterion, Throughput};
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

fn bench_sync(
    b: &mut Bencher<'_, WallTime>,
    slice_len: u64,
    prepare_len: u64,
    len: u64,
) -> Result<()> {
    let tmp = TempDir::new("bench_sync")?;
    let data = rand_bytes(len as usize);
    let path = tmp.path().join("server");
    let server = StreamStorage::open(&path, keypair([0; 32]))?;
    let id = server.create_local_stream()?;
    let mut stream = server.append(&id)?;
    stream.write_all(&data)?;
    stream.flush()?;
    let hash = stream.commit()?;
    let mut i = 0;
    b.iter(|| {
        let tmp = TempDir::new("bench_sync").unwrap();
        let path = tmp.path().join(format!("client{}", i));
        i += 1;
        let client = StreamStorage::open(&path, keypair([1; 32])).unwrap();
        client.create_replicated_stream(id.peer, id.stream).unwrap();
        let stream = client.append(&id).unwrap();
        let mut buffer = SliceBuffer::new(stream, slice_len);

        let mut slice = Vec::with_capacity(slice_len as usize * 2);
        for _ in 0..(len / prepare_len) {
            buffer.prepare(hash, prepare_len);
            for i in 0..buffer.slices().len() {
                let info = &buffer.slices()[i];
                server
                    .extract(&id, info.offset, info.len, &mut slice)
                    .unwrap();
                buffer.add_slice(&slice, i).unwrap();
                slice.clear();
            }
            buffer.commit().unwrap();
        }
    });
    Ok(())
}

fn sync_benchmark(c: &mut Criterion) {
    for len in [1024, 1024 * 1024, 1024 * 1024 * 10].iter() {
        let mut group = c.benchmark_group(format!("sync_{}", *len));
        group.throughput(Throughput::Bytes(*len));
        group.sample_size(30);

        for slice_len in [1024, 8192, 65536].iter() {
            group.bench_with_input(BenchmarkId::from_parameter(slice_len), slice_len, |b, _| {
                bench_sync(b, *slice_len, *len, *len).unwrap();
            });
        }
    }
}

criterion_group!(sync, sync_benchmark);
criterion_main!(sync);
