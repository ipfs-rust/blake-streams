use anyhow::Result;
use bao::decode::SliceDecoder;
use bao::encode::{Encoder, SliceExtractor};
use bao::Hash;
use fnv::FnvHashSet;
use parking_lot::Mutex;
use rkyv::de::deserializers::AllocDeserializer;
use rkyv::ser::serializers::AlignedSerializer;
use rkyv::ser::Serializer;
use rkyv::{AlignedVec, Archive, Deserialize, Serialize};
use std::convert::{TryFrom, TryInto};
use std::fs::{File, OpenOptions};
use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub type StreamId = u64;

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
pub struct Stream {
    hash: [u8; 32],
    outboard: Vec<u8>,
    start: u64,
    len: u64,
}

impl Default for Stream {
    fn default() -> Self {
        Self {
            hash: [
                175, 19, 73, 185, 245, 249, 161, 166, 160, 64, 77, 234, 54, 220, 201, 73, 155, 203,
                37, 201, 173, 193, 18, 183, 204, 154, 147, 202, 228, 31, 50, 98,
            ],
            outboard: vec![0, 0, 0, 0, 0, 0, 0, 0],
            start: 0,
            len: 0,
        }
    }
}

impl Stream {
    pub fn to_bytes(&self) -> Result<AlignedVec> {
        let mut ser = AlignedSerializer::new(AlignedVec::new());
        ser.serialize_value(self)?;
        Ok(ser.into_inner())
    }
}

pub struct ZeroCopy<T> {
    _marker: PhantomData<T>,
    ivec: sled::IVec,
}

impl<T: Archive> ZeroCopy<T> {
    fn new(ivec: sled::IVec) -> Self {
        Self {
            _marker: PhantomData,
            ivec,
        }
    }

    fn to_inner(&self) -> T
    where
        T::Archived: Deserialize<T, AllocDeserializer>,
    {
        let mut der = AllocDeserializer;
        self.deserialize(&mut der).unwrap()
    }
}

impl<T: Archive> Deref for ZeroCopy<T> {
    type Target = T::Archived;

    fn deref(&self) -> &Self::Target {
        unsafe { rkyv::archived_root::<T>(&self.ivec[..]) }
    }
}

impl<T: Archive> AsRef<T::Archived> for ZeroCopy<T> {
    fn as_ref(&self) -> &T::Archived {
        &*self
    }
}

pub struct StreamReader {
    file: File,
    start: u64,
    len: u64,
    pos: u64,
}

impl Read for StreamReader {
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        // read from file but within start/len bounds
        if self.pos >= self.len {
            return Ok(0);
        }
        if buf.len() as u64 >= self.len - self.pos {
            let (vbuf, _) = buf.split_at_mut((self.len - self.pos).try_into().unwrap());
            buf = vbuf;
        }
        let n = self.file.read(buf)?;
        self.pos += n as u64;
        Ok(n)
    }
}

impl Seek for StreamReader {
    fn seek(&mut self, seek: SeekFrom) -> io::Result<u64> {
        fn add_offset(position: i128, offset: i128) -> io::Result<u64> {
            let sum = position + offset;
            if sum < 0 {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "seek before beginning",
                ))
            } else if sum > u64::max_value() as i128 {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "seek target overflowed u64",
                ))
            } else {
                Ok(sum as u64)
            }
        }
        // seek from file but within start/len bounds
        let pos = match seek {
            SeekFrom::Start(start) => start,
            SeekFrom::Current(current) => add_offset(self.pos as _, current as _)?,
            SeekFrom::End(end) => add_offset(self.len as _, end as _)?,
        };
        let start = add_offset(self.start as _, pos as _)?;
        self.file.seek(SeekFrom::Start(start))?;
        self.pos = pos;
        Ok(self.pos)
    }

    fn stream_position(&mut self) -> io::Result<u64> {
        Ok(self.pos)
    }
}

pub struct StreamWriter {
    db: sled::Db,
    lock: StreamLock,
    file: File,
    encoder: Encoder<Cursor<Vec<u8>>>,
    stream: Stream,
}

impl StreamWriter {
    fn new(
        db: sled::Db,
        lock: StreamLock,
        stream: ZeroCopy<Stream>,
        path: PathBuf,
    ) -> Result<Self> {
        let mut file = OpenOptions::new().write(true).create(true).open(path)?;
        file.seek(SeekFrom::Start(stream.len))?;
        // TODO restore state of encoder
        let encoder = Encoder::new_outboard(Cursor::new(vec![]));
        Ok(StreamWriter {
            db,
            lock,
            file,
            encoder,
            stream: stream.to_inner(),
        })
    }

    pub fn commit(&mut self) -> Result<Hash> {
        let (hash, outboard) = self.encoder.clone().finalize()?;
        self.stream.hash = hash.into();
        self.stream.outboard = outboard.into_inner();
        let key = self.lock.id.to_be_bytes();
        let stream = self
            .stream
            .to_bytes()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?
            .into_vec();
        self.db.insert(&key, stream)?;
        Ok(hash)
    }
}

impl Write for StreamWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.file.write_all(buf)?;
        self.encoder.write_all(buf)?;
        self.stream.len += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.encoder.flush()?;
        Ok(())
    }
}

struct StreamLock {
    id: StreamId,
    locks: Arc<Mutex<FnvHashSet<StreamId>>>,
}

impl Drop for StreamLock {
    fn drop(&mut self) {
        let mut locks = self.locks.lock();
        debug_assert!(locks.remove(&self.id));
    }
}

pub struct StreamStorage {
    db: sled::Db,
    dir: PathBuf,
    locks: Arc<Mutex<FnvHashSet<StreamId>>>,
}

impl StreamStorage {
    pub fn open(dir: &Path) -> Result<Self> {
        let db = sled::open(dir.join("db"))?;
        let dir = dir.join("streams");
        std::fs::create_dir(&dir)?;
        Ok(Self {
            db,
            dir,
            locks: Default::default(),
        })
    }

    pub fn streams(&self) -> impl Iterator<Item = Result<(StreamId, ZeroCopy<Stream>)>> {
        self.db.iter().map(|res| {
            let (k, v) = res?;
            let id = u64::from_be_bytes(TryFrom::try_from(&k[..]).unwrap());
            Ok((id, ZeroCopy::new(v)))
        })
    }

    pub fn create_stream(&self) -> Result<u64> {
        let stream = Stream::default().to_bytes()?.into_vec();
        let id = self.db.transaction::<_, _, sled::Error>(|tx| {
            let id: StreamId = tx.generate_id()?;
            let key = id.to_be_bytes();
            tx.insert(&key, &stream[..])?;
            Ok(id)
        })?;
        Ok(id)
    }

    pub fn get_stream(&self, id: StreamId) -> Result<Option<ZeroCopy<Stream>>> {
        let key = id.to_be_bytes();
        if let Some(bytes) = self.db.get(&key)? {
            Ok(Some(ZeroCopy::new(bytes)))
        } else {
            Ok(None)
        }
    }

    fn lock_stream(&self, id: StreamId) -> Result<StreamLock> {
        if !self.locks.lock().insert(id) {
            return Err(anyhow::anyhow!("stream busy"));
        }
        Ok(StreamLock {
            id,
            locks: self.locks.clone(),
        })
    }

    pub fn append(&self, id: StreamId) -> Result<StreamWriter> {
        let lock = self.lock_stream(id)?;
        let stream = if let Some(stream) = self.get_stream(id)? {
            stream
        } else {
            return Err(anyhow::anyhow!("stream doesn't exist"));
        };
        Ok(StreamWriter::new(
            self.db.clone(),
            lock,
            stream,
            self.dir.join(id.to_string()),
        )?)
    }

    pub fn remove_stream(&self, id: StreamId) -> Result<()> {
        let _lock = self.lock_stream(id)?;
        // this is safe to do on linux as long as there are only readers.
        // the file will be deleted after the last reader is dropped.
        std::fs::remove_file(self.dir.join(id.to_string()))?;
        self.db.remove(id.to_be_bytes())?;
        Ok(())
    }

    pub fn slice(&self, id: StreamId, start: u64, len: u64) -> Result<StreamReader> {
        let stream = if let Some(stream) = self.get_stream(id)? {
            stream
        } else {
            return Err(anyhow::anyhow!("stream doesn't exist"));
        };
        if start < stream.start {
            return Err(anyhow::anyhow!(
                "trying to read before the start of the stream"
            ));
        }
        if start + len > stream.start + stream.len {
            return Err(anyhow::anyhow!(
                "trying to read after the end of the stream"
            ));
        }
        let mut file = File::open(self.dir.join(id.to_string()))?;
        file.seek(SeekFrom::Start(start - stream.start))?;
        Ok(StreamReader {
            file,
            start: start - stream.start,
            len,
            pos: 0,
        })
    }

    pub fn extract(&self, id: StreamId, start: u64, len: u64, buf: &mut Vec<u8>) -> Result<()> {
        let stream = if let Some(stream) = self.get_stream(id)? {
            stream
        } else {
            return Err(anyhow::anyhow!("stream doesn't exist"));
        };
        let file = File::open(self.dir.join(id.to_string()))?;
        let mut extractor =
            SliceExtractor::new_outboard(file, Cursor::new(&stream.outboard), start, len);
        extractor.read_to_end(buf)?;
        Ok(())
    }
}

pub struct SliceBuffer {
    stream: StreamWriter,
    hash: Hash,
    slice_len: u64,
    buf: Vec<u8>,
    slices: Vec<SliceInfo>,
    written: u64,
}

#[derive(Debug)]
pub struct SliceInfo {
    pub offset: u64,
    pub len: u64,
    pub written: bool,
}

impl SliceBuffer {
    pub fn new(stream: StreamWriter, hash: Hash, slice_len: u64) -> Self {
        Self {
            stream,
            hash,
            slice_len,
            buf: vec![],
            slices: vec![],
            written: 0,
        }
    }

    pub fn prepare(&mut self, len: u64) {
        self.slices.clear();
        self.slices.reserve((len % self.slice_len + 2) as _);
        let mut pos = self.stream.stream.start + self.stream.stream.len;
        let end = pos + len;
        if pos % self.slice_len != 0 {
            let alignment_slice = u64::min(self.slice_len - pos % self.slice_len, len);
            self.slices.push(SliceInfo {
                offset: pos,
                len: alignment_slice,
                written: false,
            });
            pos += alignment_slice;
        }
        while pos + self.slice_len < end {
            self.slices.push(SliceInfo {
                offset: pos,
                len: self.slice_len,
                written: false,
            });
            pos += self.slice_len;
        }
        if pos < end {
            let final_slice = end - pos;
            self.slices.push(SliceInfo {
                offset: pos,
                len: final_slice,
                written: false,
            });
        }
        self.buf.clear();
        self.buf.reserve(len as usize);
        unsafe { self.buf.set_len(len as usize) };
        self.written = 0;
    }

    pub fn slices(&self) -> &[SliceInfo] {
        &self.slices
    }

    pub fn add_slice(&mut self, slice: &[u8], i: usize) -> Result<()> {
        let info = &self.slices[i];
        if info.written {
            return Ok(());
        }
        let mut decoder = SliceDecoder::new(slice, &self.hash, info.offset, info.len);
        let start = info.offset - self.stream.stream.len - self.stream.stream.start;
        let end = start + info.len;
        decoder.read_exact(&mut self.buf[(start as usize)..(end as usize)])?;
        let mut end = [0u8];
        assert_eq!(decoder.read(&mut end).unwrap(), 0);
        self.slices[i].written = true;
        self.written += 1;
        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        if self.written < self.slices.len() as u64 {
            return Err(anyhow::anyhow!("missing slices"));
        }
        self.stream.write_all(&self.buf)?;
        self.stream.flush()?;
        self.stream.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bao::decode::SliceDecoder;
    use rand::RngCore;
    use std::io::BufReader;
    use tempdir::TempDir;

    fn rand_bytes(size: usize) -> Vec<u8> {
        let mut rng = rand::thread_rng();
        let mut data = Vec::with_capacity(size);
        data.resize(data.capacity(), 0);
        rng.fill_bytes(&mut data);
        data
    }

    #[test]
    fn test_default_stream() {
        let (outboard, hash) = bao::encode::outboard(&[]);
        let expect = Stream {
            hash: *hash.as_bytes(),
            outboard,
            start: 0,
            len: 0,
        };
        let actual = Stream::default();
        assert_eq!(actual, expect);
    }

    #[test]
    fn test_append_stream() -> Result<()> {
        let tmp = TempDir::new("test_append_stream")?;
        let storage = StreamStorage::open(tmp.path())?;
        let id = storage.create_stream()?;
        let data = rand_bytes(1_000_000);

        let mut stream = storage.append(id)?;
        stream.write_all(&data)?;
        stream.flush()?;
        stream.commit()?;

        let stream = storage.slice(id, 0, data.len() as u64)?;
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
        let storage = StreamStorage::open(tmp.path())?;
        let id = storage.create_stream()?;
        let data = rand_bytes(1027);
        let mut stream = storage.append(id)?;
        stream.write_all(&data)?;
        stream.flush()?;
        let hash = stream.commit()?;

        let offset = 8;
        let len = 32;
        let slice = data[offset..(offset + len)].to_vec();

        let mut stream = storage.slice(id, offset as u64, len as u64)?;
        let mut slice2 = vec![];
        stream.read_to_end(&mut slice2)?;
        assert_eq!(slice2, slice);

        let mut extracted = vec![];
        storage.extract(id, offset as u64, len as u64, &mut extracted)?;

        let mut slice2 = vec![];
        let mut decoder = SliceDecoder::new(&*extracted, &hash, offset as u64, len as u64);
        decoder.read_to_end(&mut slice2)?;
        assert_eq!(slice2, slice);
        Ok(())
    }

    #[test]
    fn test_sync() -> Result<()> {
        let tmp = TempDir::new("test_sync_1")?;
        let storage = StreamStorage::open(tmp.path())?;
        let id = storage.create_stream()?;
        let data = rand_bytes(8192);
        let mut stream = storage.append(id)?;
        stream.write_all(&data)?;
        stream.flush()?;
        let hash = stream.commit()?;

        let tmp = TempDir::new("test_sync_2")?;
        let storage2 = StreamStorage::open(tmp.path())?;
        let id = storage2.create_stream()?;
        let stream = storage2.append(id)?;
        let mut slices = SliceBuffer::new(stream, hash, 1024);

        let mut slice = vec![];
        for _ in 0..2 {
            slices.prepare(4096);
            println!("{:?}", slices.slices());
            for i in 0..slices.slices().len() {
                let info = &slices.slices()[i];
                storage.extract(id, info.offset, info.len, &mut slice)?;
                slices.add_slice(&slice, i)?;
                slice.clear();
            }
            slices.commit()?;
        }

        let mut stream = storage2.slice(id, 0, 8192)?;
        let mut data2 = vec![];
        stream.read_to_end(&mut data2)?;
        assert_eq!(data, data2);

        Ok(())
    }
}
