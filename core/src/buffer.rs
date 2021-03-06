use crate::{Hash, Head, SignedHead, Slice, StreamId, StreamWriter};
use anyhow::Result;
use bao::decode::SliceDecoder;
use fnv::FnvHashSet;
use std::io::{Read, Write};

pub struct SliceBuffer {
    stream: StreamWriter<()>,
    slice_len: u64,
    buf: Vec<u8>,
    slices: Vec<SliceInfo>,
    written: u64,
    hashes: FnvHashSet<[u8; 32]>,
}

#[derive(Debug)]
pub struct SliceInfo {
    pub offset: u64,
    pub len: u64,
    pub written: bool,
}

impl SliceBuffer {
    pub fn new(stream: StreamWriter<()>, slice_len: u64) -> Self {
        Self {
            stream,
            slice_len,
            buf: vec![],
            slices: vec![],
            written: 0,
            hashes: Default::default(),
        }
    }

    pub fn id(&self) -> &StreamId {
        self.stream.id()
    }

    pub fn head(&self) -> &SignedHead {
        self.stream.head()
    }

    pub fn prepare(&mut self, len: u64) {
        assert!(self.commitable());
        self.slices.clear();
        self.slices.reserve((len % self.slice_len + 2) as _);
        let mut pos = self.head().head().len();
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

    pub fn add_slice(&mut self, slice: &Slice, i: usize) -> Result<()> {
        let head = slice.head.head();
        if !self.hashes.contains(head.hash()) {
            slice.head.verify(self.stream.id())?;
            self.hashes.insert(*head.hash());
        }
        let info = &self.slices[i];
        if info.written {
            return Ok(());
        }
        let mut decoder = SliceDecoder::new(
            &slice.data[..],
            &Hash::from(*head.hash()),
            info.offset,
            info.len,
        );
        let start = info.offset - self.head().head().len();
        let end = start + info.len;
        decoder.read_exact(&mut self.buf[(start as usize)..(end as usize)])?;
        let mut end = [0u8];
        assert_eq!(decoder.read(&mut end).unwrap(), 0);
        self.slices[i].written = true;
        self.written += 1;
        Ok(())
    }

    pub fn commitable(&self) -> bool {
        self.written >= self.slices.len() as u64
    }

    pub fn write_buffer(&mut self) -> Result<()> {
        if !self.commitable() {
            return Err(anyhow::anyhow!("missing slices"));
        }
        self.stream.write_all(&self.buf)?;
        Ok(())
    }

    pub fn commit(&mut self, sig: [u8; 64]) -> Result<Head> {
        self.write_buffer()?;
        self.stream.flush()?;
        Ok(self.stream.commit(sig)?)
    }
}
