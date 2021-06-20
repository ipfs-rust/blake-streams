use crate::stream::StreamLock;
use crate::{SignedHead, Slice, Stream, StreamId, StreamReader, StreamWriter};
use anyhow::Result;
use bao::encode::SliceExtractor;
use ed25519_dalek::Keypair;
use fnv::FnvHashSet;
use parking_lot::Mutex;
use rkyv::{Archive, Deserialize, Infallible};
use std::fs::File;
use std::io::{Cursor, Read};
use std::marker::PhantomData;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use zerocopy::AsBytes;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ZeroCopy<T> {
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
        T::Archived: Deserialize<T, Infallible>,
    {
        self.deref().deserialize(&mut Infallible).unwrap()
    }
}

impl<T: Archive> Deref for ZeroCopy<T> {
    type Target = T::Archived;

    fn deref(&self) -> &Self::Target {
        unsafe { rkyv::archived_root::<T>(&self.ivec[..]) }
    }
}

impl<T> AsRef<[u8]> for ZeroCopy<T> {
    fn as_ref(&self) -> &[u8] {
        &self.ivec
    }
}

impl<T> From<&ZeroCopy<T>> for sled::IVec {
    fn from(zc: &ZeroCopy<T>) -> Self {
        zc.ivec.clone()
    }
}

pub struct StreamStorage {
    db: sled::Db,
    dir: PathBuf,
    key: Arc<Keypair>,
    locks: Arc<Mutex<FnvHashSet<StreamId>>>,
}

impl StreamStorage {
    pub fn open(dir: &Path, key: Keypair) -> Result<Self> {
        let db = sled::open(dir.join("db"))?;
        let dir = dir.join("streams");
        std::fs::create_dir(&dir)?;
        Ok(Self {
            db,
            dir,
            key: Arc::new(key),
            locks: Default::default(),
        })
    }

    fn get_stream(&self, id: &StreamId) -> Result<Option<ZeroCopy<Stream>>> {
        if let Some(bytes) = self.db.get(id.as_bytes())? {
            Ok(Some(ZeroCopy::new(bytes)))
        } else {
            Ok(None)
        }
    }

    fn lock_stream(&self, id: StreamId) -> Result<StreamLock> {
        if !self.locks.lock().insert(id) {
            return Err(anyhow::anyhow!("stream busy"));
        }
        Ok(StreamLock::new(id, self.locks.clone()))
    }

    pub fn streams(&self) -> impl Iterator<Item = Result<(StreamId, SignedHead)>> {
        self.db.iter().map(|res| {
            let (k, v) = res?;
            let id = ZeroCopy::<StreamId>::new(k);
            let stream = ZeroCopy::<Stream>::new(v);
            let head = stream.head.deserialize(&mut Infallible)?;
            Ok((id.to_inner(), head))
        })
    }

    pub fn create_local_stream(&self) -> Result<StreamId> {
        let peer = self.key.public.to_bytes();
        let stream = self
            .db
            .transaction::<_, _, sled::Error>(|tx| Ok(tx.generate_id()?))?;
        let id = StreamId::new(peer, stream);
        self.create_replicated_stream(&id)?;
        Ok(id)
    }

    pub fn create_replicated_stream(&self, id: &StreamId) -> Result<()> {
        let stream = Stream::new(*id).to_bytes()?.into_vec();
        self.db.insert(id.as_bytes(), &stream[..])?;
        Ok(())
    }

    pub fn append_local_stream(&self, id: &StreamId) -> Result<StreamWriter<Arc<Keypair>>> {
        let lock = self.lock_stream(id.clone())?;
        let stream = if let Some(stream) = self.get_stream(id)? {
            stream
        } else {
            return Err(anyhow::anyhow!("stream doesn't exist"));
        };
        Ok(StreamWriter::new(
            self.dir.join(id.to_string()),
            stream.to_inner(),
            lock,
            self.db.clone(),
            self.key.clone(),
        )?)
    }

    pub fn append_replicated_stream(&self, id: &StreamId) -> Result<StreamWriter<()>> {
        let lock = self.lock_stream(id.clone())?;
        let stream = if let Some(stream) = self.get_stream(id)? {
            stream
        } else {
            return Err(anyhow::anyhow!("stream doesn't exist"));
        };
        Ok(StreamWriter::new(
            self.dir.join(id.to_string()),
            stream.to_inner(),
            lock,
            self.db.clone(),
            (),
        )?)
    }

    pub fn remove_stream(&self, id: &StreamId) -> Result<()> {
        let _lock = self.lock_stream(id.clone())?;
        // this is safe to do on linux as long as there are only readers.
        // the file will be deleted after the last reader is dropped.
        std::fs::remove_file(self.dir.join(id.to_string()))?;
        self.db.remove(id.as_bytes())?;
        Ok(())
    }

    pub fn slice(&self, id: &StreamId, start: u64, len: u64) -> Result<StreamReader> {
        let stream = if let Some(stream) = self.get_stream(id)? {
            stream
        } else {
            return Err(anyhow::anyhow!("stream doesn't exist"));
        };
        let path = self.dir.join(id.to_string());
        StreamReader::new(path, &stream.head.head, start, len)
    }

    pub fn extract(&self, id: &StreamId, start: u64, len: u64, slice: &mut Slice) -> Result<()> {
        slice.data.clear();
        let stream = if let Some(stream) = self.get_stream(id)? {
            stream
        } else {
            return Err(anyhow::anyhow!("stream doesn't exist"));
        };
        let file = File::open(self.dir.join(id.to_string()))?;
        slice.head = stream.head;
        let mut extractor =
            SliceExtractor::new_outboard(file, Cursor::new(&stream.outboard), start, len);
        extractor.read_to_end(&mut slice.data)?;
        Ok(())
    }
}
