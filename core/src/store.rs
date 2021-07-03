use crate::stream::StreamLock;
use crate::{Head, Slice, Stream, StreamId, StreamReader, StreamWriter};
use anyhow::Result;
use bao::encode::SliceExtractor;
use ed25519_dalek::{Keypair, PublicKey};
use fnv::{FnvHashMap, FnvHashSet};
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
    paths: FnvHashMap<StreamId, PathBuf>,
}

impl StreamStorage {
    pub fn open(dir: &Path, key: Keypair) -> Result<Self> {
        let db = sled::open(dir.join("db"))?;
        let dir = dir.join("streams");
        std::fs::create_dir_all(&dir)?;
        Ok(Self {
            db,
            dir,
            key: Arc::new(key),
            locks: Default::default(),
            paths: Default::default(),
        })
    }

    pub fn public_key(&self) -> &PublicKey {
        &self.key.public
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

    fn stream_path(&mut self, id: &StreamId) -> &Path {
        if !self.paths.contains_key(id) {
            self.paths.insert(*id, self.dir.join(id.to_string()));
        }
        self.paths.get(id).unwrap()
    }

    pub fn streams(&self) -> impl Iterator<Item = Result<StreamId>> {
        self.db
            .iter()
            .keys()
            .map(|res| Ok(ZeroCopy::<StreamId>::new(res?).to_inner()))
    }

    pub fn contains(&self, id: &StreamId) -> Result<bool> {
        Ok(self.db.contains_key(id.as_bytes())?)
    }

    pub fn head(&self, id: &StreamId) -> Result<Option<Head>> {
        if let Some(stream) = self.db.get(id.as_bytes())? {
            let stream = ZeroCopy::<Stream>::new(stream);
            let head = stream.head.head().deserialize(&mut Infallible)?;
            return Ok(Some(head));
        }
        Ok(None)
    }

    pub fn append(&mut self, stream: u64) -> Result<StreamWriter<Arc<Keypair>>> {
        let peer = self.key.public.to_bytes();
        let id = StreamId::new(peer, stream);
        if !self.contains(&id)? {
            let stream = Stream::new(id).to_bytes()?.into_vec();
            self.db.insert(id.as_bytes(), &stream[..])?;
        }
        let lock = self.lock_stream(id.clone())?;
        let stream = if let Some(stream) = self.get_stream(&id)? {
            stream
        } else {
            return Err(anyhow::anyhow!("stream doesn't exist"));
        };
        let db = self.db.clone();
        let key = self.key.clone();
        let path = self.stream_path(&id);
        Ok(StreamWriter::new(path, stream.to_inner(), lock, db, key)?)
    }

    pub fn subscribe(&mut self, id: &StreamId) -> Result<StreamWriter<()>> {
        if !self.contains(id)? {
            let stream = Stream::new(*id).to_bytes()?.into_vec();
            self.db.insert(id.as_bytes(), &stream[..])?;
        }
        let lock = self.lock_stream(id.clone())?;
        let stream = if let Some(stream) = self.get_stream(id)? {
            stream
        } else {
            return Err(anyhow::anyhow!("stream doesn't exist"));
        };
        let db = self.db.clone();
        let path = self.stream_path(id);
        Ok(StreamWriter::new(path, stream.to_inner(), lock, db, ())?)
    }

    pub fn remove(&mut self, id: &StreamId) -> Result<()> {
        let _lock = self.lock_stream(id.clone())?;
        // this is safe to do on linux as long as there are only readers.
        // the file will be deleted after the last reader is dropped.
        std::fs::remove_file(self.stream_path(id))?;
        self.db.remove(id.as_bytes())?;
        Ok(())
    }

    pub fn slice(&mut self, id: &StreamId, start: u64, len: u64) -> Result<StreamReader> {
        let stream = if let Some(stream) = self.get_stream(id)? {
            stream
        } else {
            return Err(anyhow::anyhow!("stream doesn't exist"));
        };
        StreamReader::new(self.stream_path(id), &stream.head.head, start, len)
    }

    pub fn extract(
        &mut self,
        id: &StreamId,
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
        if len > stream.head.head().len() {
            return Err(anyhow::anyhow!("trying to read after current head"));
        }
        let file = File::open(self.stream_path(id))?;
        slice.head = stream.head;
        let mut extractor =
            SliceExtractor::new_outboard(file, Cursor::new(&stream.outboard), start, len);
        extractor.read_to_end(&mut slice.data)?;
        Ok(())
    }
}
