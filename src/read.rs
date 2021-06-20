use crate::Head;
use anyhow::Result;
use std::convert::TryInto;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

pub struct StreamReader {
    file: File,
    start: u64,
    len: u64,
    pos: u64,
}

impl StreamReader {
    pub(crate) fn new(path: &Path, head: &Head, start: u64, len: u64) -> Result<Self> {
        if start + len > head.len {
            return Err(anyhow::anyhow!(
                "trying to read after the end of the stream"
            ));
        }
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(start))?;
        Ok(Self {
            file,
            start,
            len,
            pos: 0,
        })
    }
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
