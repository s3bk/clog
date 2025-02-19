use std::io;

use better_io::BetterBufRead;
use brotli::CustomRead;

use crate::Pos;


pub struct IoWritePos<W> {
    pub writer: W,
    pub pos: usize
}
impl<W: io::Write> io::Write for IoWritePos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.writer.write(buf)?;
        self.pos += n;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
    fn write_all(&mut self, mut buf: &[u8]) -> io::Result<()> {
        self.writer.write_all(buf)?;
        self.pos += buf.len();
        Ok(())
    }
}
impl<W> Pos for IoWritePos<W> {
    fn pos(&self) -> usize {
        self.pos
    }
}
impl Pos for Vec<u8> {
    fn pos(&self) -> usize {
        self.len()
    }
}

pub struct ReadAdapter<'a> {
    slice: &'a [u8],
    pos: usize,
}
impl<'a> BetterBufRead for ReadAdapter<'a> {
    fn buffer(&self) -> &[u8] {
        self.slice
    }
    fn capacity(&self) -> Option<usize> {
        None
    }
    fn consume(&mut self, n_bytes: usize) {
        self.slice = &self.slice[n_bytes..];
        self.pos += n_bytes;
    }
    fn fill_or_eof(&mut self, n_bytes: usize) -> io::Result<()> {
        Ok(())
    }
    fn resize_capacity(&mut self, desired: usize) {
        
    }
}
impl<'a> ReadAdapter<'a> {
    pub fn new(slice: &'a [u8]) -> Self {
        ReadAdapter { slice, pos: 0 }
    }
}
impl<'a> Pos for ReadAdapter<'a> {
    fn pos(&self) -> usize {
        self.pos
    }
}

pub struct BrotliReadAdapter<R> {
    pub inner: R,
    pub remaining: usize,
}

impl<R: BetterBufRead> CustomRead<io::Error> for BrotliReadAdapter<R> {
    fn read(self: &mut Self, data: &mut [u8]) -> Result<usize, io::Error> {
        let mut n = data.len().min(self.remaining);
        if n <= self.inner.buffer().len() {
            data[..n].copy_from_slice(&self.inner.buffer()[.. n]);
            self.inner.consume(n);
            self.remaining -= n;
            //println!("read {n} bytes");
            return Ok(n)
        }
        if let Some(max) = self.inner.capacity() {
            n = n.min(max);
        }
        self.inner.fill_or_eof(n)?;
        let buf = self.inner.buffer();
        n = buf.len().min(data.len()).min(self.remaining);
        data[.. n].copy_from_slice(&buf[.. n]);
        self.inner.consume(n);
        self.remaining -= n;
        //println!("read and fill {n} bytes");
        Ok(n)
    }
}
