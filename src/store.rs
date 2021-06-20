use crate::{Slice, Stream, StreamId, StreamReader, StreamWriter};
use anyhow::Result;
use bao::encode::SliceExtractor;
use ed25519_dalek::PublicKey;
use fnv::FnvHashSet;
use parking_lot::Mutex;
use rkyv::de::deserializers::AllocDeserializer;
use rkyv::{Archive, Deserialize};
use std::fs::File;
use std::io::{Cursor, Read};
use std::marker::PhantomData;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
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

    pub fn to_inner(&self) -> T
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

impl<T> std::fmt::Display for ZeroCopy<T>
where
    T: Archive + std::fmt::Display,
    T::Archived: Deserialize<T, AllocDeserializer>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.to_inner().fmt(f)
    }
}

pub(crate) struct StreamLock {
    id: ZeroCopy<StreamId>,
    locks: Arc<Mutex<FnvHashSet<ZeroCopy<StreamId>>>>,
}

impl StreamLock {
    pub(crate) fn id(&self) -> &ZeroCopy<StreamId> {
        &self.id
    }
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
    key: PublicKey,
    locks: Arc<Mutex<FnvHashSet<ZeroCopy<StreamId>>>>,
}

impl StreamStorage {
    pub fn open(dir: &Path, key: PublicKey) -> Result<Self> {
        let db = sled::open(dir.join("db"))?;
        let dir = dir.join("streams");
        std::fs::create_dir(&dir)?;
        Ok(Self {
            db,
            dir,
            key,
            locks: Default::default(),
        })
    }

    pub fn streams(&self) -> impl Iterator<Item = Result<(ZeroCopy<StreamId>, ZeroCopy<Stream>)>> {
        self.db.iter().map(|res| {
            let (k, v) = res?;
            Ok((ZeroCopy::new(k), ZeroCopy::new(v)))
        })
    }

    pub fn create_local_stream(&self) -> Result<ZeroCopy<StreamId>> {
        let peer = self.key.to_bytes();
        let stream = self
            .db
            .transaction::<_, _, sled::Error>(|tx| Ok(tx.generate_id()?))?;
        self.create_replicated_stream(peer, stream)
    }

    pub fn create_replicated_stream(
        &self,
        peer: [u8; 32],
        stream: u64,
    ) -> Result<ZeroCopy<StreamId>> {
        let key = ZeroCopy::new(sled::IVec::from(
            StreamId { peer, stream }.to_bytes()?.into_vec(),
        ));
        let stream = Stream::new(stream).to_bytes()?.into_vec();
        self.db.insert(&key, &stream[..])?;
        Ok(key)
    }

    pub fn get_stream(&self, id: &ZeroCopy<StreamId>) -> Result<Option<ZeroCopy<Stream>>> {
        if let Some(bytes) = self.db.get(id)? {
            Ok(Some(ZeroCopy::new(bytes)))
        } else {
            Ok(None)
        }
    }

    fn lock_stream(&self, id: ZeroCopy<StreamId>) -> Result<StreamLock> {
        if !self.locks.lock().insert(id.clone()) {
            return Err(anyhow::anyhow!("stream busy"));
        }
        Ok(StreamLock {
            id,
            locks: self.locks.clone(),
        })
    }

    pub fn append(&self, id: &ZeroCopy<StreamId>) -> Result<StreamWriter> {
        let lock = self.lock_stream(id.clone())?;
        let stream = if let Some(stream) = self.get_stream(id)? {
            stream
        } else {
            return Err(anyhow::anyhow!("stream doesn't exist"));
        };
        Ok(StreamWriter::new(
            self.dir.join(id.to_string()),
            stream,
            lock,
            self.db.clone(),
        )?)
    }

    pub fn remove_stream(&self, id: &ZeroCopy<StreamId>) -> Result<()> {
        let _lock = self.lock_stream(id.clone())?;
        // this is safe to do on linux as long as there are only readers.
        // the file will be deleted after the last reader is dropped.
        std::fs::remove_file(self.dir.join(id.to_string()))?;
        self.db.remove(id)?;
        Ok(())
    }

    pub fn slice(&self, id: &ZeroCopy<StreamId>, start: u64, len: u64) -> Result<StreamReader> {
        let stream = if let Some(stream) = self.get_stream(id)? {
            stream
        } else {
            return Err(anyhow::anyhow!("stream doesn't exist"));
        };
        let path = self.dir.join(id.to_string());
        StreamReader::new(path, stream, start, len)
    }

    pub fn extract(
        &self,
        id: &ZeroCopy<StreamId>,
        start: u64,
        len: u64,
        slice: &mut Slice,
    ) -> Result<()> {
        slice.data.clear();
        let stream = if let Some(stream) = self.get_stream(id)? {
            stream
        } else {
            return Err(anyhow::anyhow!("stream doesn't exist"));
        };
        let file = File::open(self.dir.join(id.to_string()))?;
        let mut head = slice.head.head_mut();
        head.stream = stream.head.stream;
        head.hash = stream.head.hash;
        head.len = stream.head.len;
        let mut extractor =
            SliceExtractor::new_outboard(file, Cursor::new(&stream.outboard), start, len);
        extractor.read_to_end(&mut slice.data)?;
        Ok(())
    }
}
