use crate::store::StreamLock;
use crate::{Head, Stream, StreamId, ZeroCopy};
use anyhow::Result;
use bao::encode::Encoder;
use std::fs::{File, OpenOptions};
use std::io::{self, Cursor, Seek, SeekFrom, Write};
use std::path::PathBuf;

pub struct StreamWriter {
    db: sled::Db,
    lock: StreamLock,
    file: File,
    encoder: Encoder<Cursor<Vec<u8>>>,
    stream: Stream,
}

impl StreamWriter {
    pub(crate) fn new(
        path: PathBuf,
        stream: ZeroCopy<Stream>,
        lock: StreamLock,
        db: sled::Db,
    ) -> Result<Self> {
        let mut file = OpenOptions::new().write(true).create(true).open(path)?;
        file.seek(SeekFrom::Start(stream.head.len))?;
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

    pub fn id(&self) -> &ZeroCopy<StreamId> {
        self.lock.id()
    }

    pub fn head(&self) -> &Head {
        &self.stream.head
    }

    pub fn commit(&mut self) -> Result<Head> {
        let (hash, outboard) = self.encoder.clone().finalize()?;
        self.stream.head.hash = hash.into();
        self.stream.outboard = outboard.into_inner();
        let stream = self
            .stream
            .to_bytes()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?
            .into_vec();
        self.db.insert(self.id(), stream)?;
        Ok(self.stream.head)
    }
}

impl Write for StreamWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.file.write_all(buf)?;
        self.encoder.write_all(buf)?;
        self.stream.head.len += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.encoder.flush()?;
        Ok(())
    }
}
