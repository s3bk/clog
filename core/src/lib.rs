#![feature(alloc_layout_extra)]

use std::ops::Deref;
use std::{fs::File, io, net::IpAddr, usize};
use std::io::{BufReader, BufWriter, BufRead, Write};

use better_io::BetterBufRead;
use bytes::{BufMut, Bytes, BytesMut};
use http::header::{REFERER, USER_AGENT};
use http::HeaderName;
use istring::SmallString;
use itertools::intersperse;
use pco::wrapped::{FileCompressor, FileDecompressor};
use anyhow::{Error};
use serde::{Deserialize, Serialize};
use shema::{BatchEntry, Shema};
use slice::SliceTrait;
use strum::FromRepr;

pub mod util;
pub mod shema;
pub mod types;
pub mod filter;
mod slice;

#[cfg(all(target_feature="aes", target_feature="sse2"))]
pub type BuildHasher = gxhash::GxBuildHasher;

#[cfg(not(all(target_feature="aes", target_feature="sse2")))]
pub type BuildHasher = rapidhash::RapidBuildHasher;

#[cfg(feature="encode")]
#[derive(serde::Serialize, serde::Deserialize)]
pub struct RequestEntry {
    pub status: u16,
    pub method: SmallString,
    pub uri: String,
    #[serde(default)]
    pub user_agent: Option<String>,
    #[serde(default)]
    pub referer: Option<String>,
    pub ip: IpAddr,
    pub port: u16,
    #[serde(default)]
    pub time: u64,
    #[serde(default)]
    pub body: Option<Bytes>,
    #[serde(default)]
    pub headers: Headers,
}


#[derive(serde::Serialize, serde::Deserialize, Default)]
pub struct Headers(pub String);

pub fn headers_string<'a>(pairs: impl Iterator<Item=(&'a str, &'a str)>) -> String {
    let mut out = String::new();
    for (i, (k, v)) in pairs.enumerate() {
        if i > 0 {
            out.push('\n');
        }
        out.push_str(k);
        out.push(':');
        out.push_str(v);
    }
    out
}

const SKIP_HEADERS: &[HeaderName] = &[REFERER, USER_AGENT];
impl<'a> From<&'a http::HeaderMap> for Headers {
    fn from(map: &'a http::HeaderMap) -> Self {
        let pairs = map.iter()
            .filter(|(k, _)| !SKIP_HEADERS.contains(k))
            .filter_map(|(k, v)| Some((k.as_str(), v.to_str().ok()?)));

        Headers(headers_string(pairs))
    }
}
impl Headers {
    pub fn split(&self) -> Vec<(&str, &str)> {
        self.0.split("\n").filter_map(|s| s.split_once(":")).collect()
    }
}

#[derive(Serialize, Deserialize)]
pub struct BatchHeader {
    pub start: u64
}

#[derive(Serialize, Deserialize)]
pub struct SyncHeader {
    pub start: u64,
    pub block_size: usize,
    pub first_block: u64,
    pub first_backlog: u64,
}


#[derive(Copy, Clone, FromRepr)]
#[repr(u8)]
pub enum PacketType {
    Batch = 1,
    Row = 2,
    Sync = 3,
    ServerMsg = 4,
}
impl PacketType {
    pub fn write_to(&self, buf: &mut BytesMut) {
        buf.put_u8(*self as u8);
    }
    pub fn parse(byte: u8) -> Option<Self> {
        Self::from_repr(byte)
    }
}

pub trait Pos {
    fn pos(&self) -> usize;
}

#[derive(Clone)]
pub struct Input<'a> {
    data: &'a [u8],
    pos: usize
}
impl<'a> Input<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Input { data, pos: 0 }
    }
    #[inline(always)]
    pub fn advance(&mut self, n: usize) {
        self.data = &self.data[n..];
        self.pos += n;
    }
    pub fn take_n(&mut self, n: usize) -> Result<&'a [u8], Error> {
        let (out, rest) = self.data.split_at_checked(n).ok_or_else(|| anyhow::anyhow!("not enough input data"))?;
        self.data = rest;
        self.pos += n;
        Ok(out)
    }
    pub fn pos(&self) -> usize {
        self.pos
    }
}
impl<'a> Deref for Input<'a> {
    type Target = [u8];
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.data
    }
}
impl<'a> BetterBufRead for Input<'a> {
    #[inline(always)]
    fn buffer(&self) -> &[u8] {
        self.data
    }
    #[inline(always)]
    fn capacity(&self) -> Option<usize> {
        None
    }
    #[inline(always)]
    fn consume(&mut self, n_bytes: usize) {
        self.advance(n_bytes);
    }
    #[inline(always)]
    fn fill_or_eof(&mut self, n_bytes: usize) -> io::Result<()> {
        Ok(())
    }
    #[inline(always)]
    fn resize_capacity(&mut self, desired: usize) {
    }
}

pub trait DataBuilder: Sized {
    type CompressedItem;
    type Item<'a>;
    type Slice<'a>;
    type SliceMut<'a>;
    type Size;
    type Data: SliceTrait;
    
    fn add<'a>(&mut self, item: Self::Item<'a>) -> Self::CompressedItem;
    fn read<'a, 'r>(f: &FileDecompressor, slice: Self::SliceMut<'a>, data: Input<'r>, size: Self::Size) -> Result<(Self, Input<'r>), Error>;
    fn get<'a>(&'a self, compressed: Self::CompressedItem) -> Option<Self::Item<'a>>;
}

#[cfg(feature="encode")]
pub trait DataBuilderEncode: DataBuilder {
    fn write<'a, W: io::Write + Pos>(&self, f: &FileCompressor, slice: Self::Slice<'a>, writer: W, opt: &Options) -> Result<(Self::Size, W), Error>;
}


#[derive(Default)]
pub struct Options {
    pub brotli_level: u8,
    pub dict: &'static [u8]
}
