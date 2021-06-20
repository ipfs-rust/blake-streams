pub use crate::buffer::{SliceBuffer, SliceInfo};
pub use crate::read::StreamReader;
pub use crate::store::{StreamStorage, ZeroCopy};
pub use crate::stream::{Head, SignedHead, Slice, Stream, StreamId};
pub use crate::write::StreamWriter;
pub use bao::Hash;

mod buffer;
mod read;
mod store;
mod stream;
mod write;

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use bao::decode::SliceDecoder;
    use ed25519_dalek::{Keypair, PublicKey, SecretKey};
    use rand::RngCore;
    use std::io::{BufReader, Read, Write};
    use tempdir::TempDir;

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

    #[test]
    fn test_append_stream() -> Result<()> {
        let tmp = TempDir::new("test_append_stream")?;
        let storage = StreamStorage::open(tmp.path(), keypair([0; 32]).public)?;
        let id = storage.create_local_stream()?;
        let data = rand_bytes(1_000_000);

        let mut stream = storage.append(&id)?;
        stream.write_all(&data)?;
        stream.flush()?;
        stream.commit()?;

        let stream = storage.slice(&id, 0, data.len() as u64)?;
        let mut reader = BufReader::new(stream);
        let mut data2 = Vec::with_capacity(data.len());
        data2.resize(data2.capacity(), 0);
        reader.read_exact(&mut data2)?;
        assert_eq!(data, data2);

        Ok(())
    }

    #[test]
    fn test_extract_slice() -> Result<()> {
        let tmp = TempDir::new("test_extract_slice")?;
        let key = keypair([0; 32]);
        let storage = StreamStorage::open(tmp.path(), key.public)?;
        let id = storage.create_local_stream()?;
        let data = rand_bytes(1027);
        let mut stream = storage.append(&id)?;
        stream.write_all(&data)?;
        stream.flush()?;
        stream.commit()?;

        let offset = 8;
        let len = 32;
        let slice = data[offset..(offset + len)].to_vec();

        let mut stream = storage.slice(&id, offset as u64, len as u64)?;
        let mut slice2 = vec![];
        stream.read_to_end(&mut slice2)?;
        assert_eq!(slice2, slice);

        let mut vslice = Slice::default();
        storage.extract(&id, offset as u64, len as u64, &mut vslice)?;

        let mut slice2 = vec![];
        let head = vslice.head.verify(&id)?;
        let mut decoder = SliceDecoder::new(
            &vslice.data[..],
            &Hash::from(head.hash),
            offset as u64,
            len as u64,
        );
        decoder.read_to_end(&mut slice2)?;
        assert_eq!(slice2, slice);
        Ok(())
    }

    #[test]
    fn test_sync() -> Result<()> {
        let tmp = TempDir::new("test_sync_1")?;
        let key = keypair([0; 32]);
        let storage = StreamStorage::open(tmp.path(), key.public)?;
        let id = storage.create_local_stream()?;
        let data = rand_bytes(8192);
        let mut stream = storage.append(&id)?;
        stream.write_all(&data[..4096])?;
        stream.flush()?;
        let head1 = stream.commit()?.sign(&key)?;
        stream.write_all(&data[4096..])?;
        stream.flush()?;
        let head2 = stream.commit()?.sign(&key)?;

        let tmp = TempDir::new("test_sync_2")?;
        let key2 = keypair([1; 32]);
        let storage2 = StreamStorage::open(tmp.path(), key2.public)?;
        let id = storage2.create_replicated_stream(id.peer, id.stream)?;
        let stream = storage2.append(&id)?;
        let mut buffer = SliceBuffer::new(stream, 1024);

        let mut slice = Slice::default();
        for head in [head1, head2].iter() {
            let head = head.verify(&id)?;
            buffer.prepare(head.len - buffer.head().len);
            println!("{:?}", buffer.slices());
            for i in 0..buffer.slices().len() {
                let info = &buffer.slices()[i];
                storage.extract(&id, info.offset, info.len, &mut slice)?;
                println!("extracted");
                buffer.add_slice(&slice, i)?;
                println!("verified");
            }
            buffer.commit()?;
        }

        let mut stream = storage2.slice(&id, 0, 8192)?;
        let mut data2 = vec![];
        stream.read_to_end(&mut data2)?;
        assert_eq!(data, data2);

        Ok(())
    }
}
