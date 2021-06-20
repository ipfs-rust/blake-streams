use crate::stream::StreamLock;
use crate::{Head, SignedHead, Stream, StreamId};
use anyhow::Result;
use bao::encode::Encoder;
use ed25519_dalek::Keypair;
use std::fs::{File, OpenOptions};
use std::io::{self, Cursor, Read, Write};
use std::path::Path;
use std::sync::Arc;
use zerocopy::AsBytes;

pub struct StreamWriter<T> {
    db: sled::Db,
    _lock: StreamLock,
    file: File,
    encoder: Encoder<Cursor<Vec<u8>>>,
    stream: Stream,
    key: T,
}

impl<T> StreamWriter<T> {
    pub(crate) fn new(
        path: &Path,
        stream: Stream,
        lock: StreamLock,
        db: sled::Db,
        key: T,
    ) -> Result<Self> {
        let mut file = OpenOptions::new().write(true).create(true).open(path)?;
        let outboard = Vec::with_capacity(stream.outboard.len() * 2);
        let mut encoder = Encoder::new_outboard(Cursor::new(outboard));
        // TODO: this can probably be optimized: discuss with bao author.
        let mut pos = 0;
        let mut buf = [0u8; 8192];
        while pos < stream.head.head.len {
            let npos = u64::min(pos + buf.len() as u64, stream.head.head.len);
            let n = (npos - pos) as usize;
            file.read_exact(&mut buf[..n])?;
            encoder.write_all(&mut buf[..n])?;
            pos = npos;
        }
        Ok(StreamWriter {
            db,
            _lock: lock,
            file,
            encoder,
            stream,
            key,
        })
    }

    pub fn id(&self) -> &StreamId {
        self.stream.head().id()
    }

    pub fn head(&self) -> &SignedHead {
        &self.stream.head
    }

    fn finalize(&mut self) -> io::Result<()> {
        let (hash, outboard) = self.encoder.clone().finalize()?;
        self.stream.head.head.hash = hash.into();
        self.stream.outboard = outboard.into_inner();
        Ok(())
    }

    fn save(&mut self) -> io::Result<()> {
        let stream = self
            .stream
            .to_bytes()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?
            .into_vec();
        self.db.insert(self.id().as_bytes(), stream)?;
        Ok(())
    }
}

impl StreamWriter<()> {
    pub fn commit(&mut self, sig: [u8; 64]) -> io::Result<Head> {
        self.finalize()?;
        self.stream
            .head
            .set_signature(sig)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
        self.save()?;
        Ok(self.stream.head.head)
    }
}

impl StreamWriter<Arc<Keypair>> {
    pub fn commit(&mut self) -> io::Result<SignedHead> {
        self.finalize()?;
        self.stream.head.sign(&self.key);
        self.save()?;
        Ok(self.stream.head)
    }
}

impl<T> Write for StreamWriter<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.file.write_all(buf)?;
        self.encoder.write_all(buf)?;
        self.stream.head.head.len += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.encoder.flush()?;
        Ok(())
    }
}
