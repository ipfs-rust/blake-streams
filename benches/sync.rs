use anyhow::Result;
use blake_streams::{SliceBuffer, StreamStorage};
use criterion::measurement::WallTime;
use criterion::{criterion_group, criterion_main, Bencher, BenchmarkId, Criterion, Throughput};
use rand::RngCore;
use std::io::Write;
use tempdir::TempDir;

fn rand_bytes(size: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let mut data = vec![0; size];
    rng.fill_bytes(&mut data);
    data
}

fn bench_sync(b: &mut Bencher<'_, WallTime>, slice_len: u64, len: u64) -> Result<()> {
    let tmp = TempDir::new("bench_sync")?;
    let data = rand_bytes(len as usize);
    let path = tmp.path().join("server");
    let server = StreamStorage::open(&path)?;
    let server_sid = server.create_stream()?;
    let mut stream = server.append(server_sid)?;
    stream.write_all(&data)?;
    stream.flush()?;
    let hash = stream.commit()?;
    let mut i = 0;
    b.iter(|| {
        let path = tmp.path().join(format!("client{}", i));
        i += 1;
        let client = StreamStorage::open(&path).unwrap();
        let client_sid = client.create_stream().unwrap();
        let stream = client.append(client_sid).unwrap();
        let mut buffer = SliceBuffer::new(stream, hash, len, slice_len);

        let mut slice = Vec::with_capacity(slice_len as usize * 2);
        for i in 0..buffer.slices().len() {
            let info = &buffer.slices()[i];
            server
                .extract(server_sid, info.offset, info.len, &mut slice)
                .unwrap();
            buffer.add_slice(&slice, i).unwrap();
            slice.clear();
        }
        buffer.commit().unwrap();
    });
    Ok(())
}

fn sync_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync");
    let len = 1024 * 1024;
    group.throughput(Throughput::Bytes(len));

    for slice_len in [1024, 8192, 65536].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(slice_len), slice_len, |b, _| {
            bench_sync(b, *slice_len, len).unwrap();
        });
    }
}

criterion_group!(sync, sync_benchmark);
criterion_main!(sync);
