use std::io;

use better_io::BetterBufRead;
use brotli_decompressor::CustomRead;
use bytes::BytesMut;

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
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
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

pub struct WriteAdapter(pub BytesMut);
impl std::io::Write for WriteAdapter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.extend_from_slice(buf);
        Ok(buf.len())
    }
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.0.extend_from_slice(buf);
        Ok(())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub struct ReadAdapter<'a> {
    slice: &'a [u8],
    pos: usize,
}
impl<'a> BetterBufRead for ReadAdapter<'a> {
    fn capacity(&self) -> Option<usize> {
        None
    }
    fn consume(&mut self, n_bytes: usize) {
        self.slice = &self.slice[n_bytes..];
        self.pos += n_bytes;
    }
    fn fill_or_eof(&mut self, n_bytes: usize) -> io::Result<&[u8]> {
        Ok(self.slice)
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
        if let Ok(buffer) = self.inner.fill_or_eof(n) {
            data[..n].copy_from_slice(&buffer[.. n]);
            self.inner.consume(n);
            self.remaining -= n;
            //println!("read {n} bytes");
            return Ok(n)
        }
        if let Some(max) = self.inner.capacity() {
            n = n.min(max);
        }
        let buf = self.inner.fill_or_eof(n)?;
        n = buf.len().min(data.len()).min(self.remaining);
        data[.. n].copy_from_slice(&buf[.. n]);
        self.inner.consume(n);
        self.remaining -= n;
        //println!("read and fill {n} bytes");
        Ok(n)
    }
}
