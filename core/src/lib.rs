use std::{fs::File, io, net::IpAddr, usize};
use std::io::{BufReader, BufWriter, BufRead, Write};

use better_io::BetterBufRead;
use bytes::{BufMut, BytesMut};
use istring::SmallString;
use pco::wrapped::{FileCompressor, FileDecompressor};
use anyhow::{Error};
use serde::{Deserialize, Serialize};
use shema::{BatchEntry, Builder};
use strum::FromRepr;
use types::compress_string;
use util::IoWritePos;

mod util;
pub mod shema;
mod types;
pub mod collector;
pub mod filter;

#[cfg(target_arch = "wasm32")]
type BuildHasher = rapidhash::RapidBuildHasher;

#[cfg(not(target_arch = "wasm32"))]
type BuildHasher = gxhash::GxBuildHasher;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct RequestEntry {
    pub status: u16,
    pub method: SmallString,
    pub uri: String,
    pub user_agent: Option<String>,
    pub referer: Option<String>,
    pub ip: IpAddr,
    pub port: u16,
    pub time: u64,
}

#[derive(Serialize, Deserialize)]
pub struct BatchHeader {
    pub start: u64
}

#[derive(Copy, Clone, FromRepr)]
#[repr(u8)]
pub enum PacketType {
    Batch = 1,
    Row = 2,
    Sync = 3,
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

pub trait DataBuilder: Sized {
    type CompressedItem;
    type Item<'a>;
    type Slice<'a>;
    type SliceMut<'a>;
    type Size;
    
    fn add<'a>(&mut self, item: Self::Item<'a>) -> Self::CompressedItem;
    fn write<'a, W: io::Write + Pos>(&self, f: &FileCompressor, slice: Self::Slice<'a>, writer: W, opt: &Options) -> Result<(Self::Size, W), Error>;
    fn read<'a, R: BetterBufRead + Pos>(f: &FileDecompressor, slice: Self::SliceMut<'a>, reader: R, size: Self::Size) -> Result<(Self, R), Error>;
    fn get<'a>(&'a self, compressed: Self::CompressedItem) -> Option<Self::Item<'a>>;
}

fn read_log() -> impl Iterator<Item=RequestEntry> {
    let file = File::open("../artisan/user.log").unwrap();
    let mut reader = BufReader::new(file);

    let mut line = String::new();

    std::iter::from_fn(move || {
        let n = reader.read_line(&mut line).ok()?;
        if n == 0 {
            return None;
        }
        let out = serde_json::from_str::<RequestEntry>(&line);
        line.clear();
        Some(out)
    }).flat_map(|r| r.ok())
}

#[test]
fn test_log() {
    let mut builder = Builder::default();
    let entries: Vec<RequestEntry> = read_log().collect();
    for entry in entries.iter() {
        builder.add(entry.into());
    }
    println!("parsing complete");

    for q in 1 .. 12 {
        let opt = Options {
            brotli_level: q, .. Default::default()
        };
        let data = builder.to_vec(&opt);
        println!("q={q}, size={}", data.len());
    }

    let data = builder.to_vec(&Options::default());
    println!("compressed: {} bytes", data.len());
    std::fs::write("user.data", &data).unwrap();

    println!("{} bytes per row", data.len() as f64 / builder.len() as f64);
    let b2 = Builder::from_slice(&data);

    /*
    for item in b2.iter() {
        println!("{item:?}");
    }
     */
}

#[derive(Default)]
pub struct Options {
    pub brotli_level: u8,
    pub dict: &'static [u8]
}

fn test_dict(uris: &str, opt: &Options) -> usize {
    let mut out = IoWritePos { writer: vec![], pos: 0 };
    compress_string(&mut out, uris, &opt).unwrap();
    out.writer.len()
}

#[test]
fn test_compression() {
    use std::collections::HashSet;
    let mut uris = HashSet::with_hasher(BuildHasher::default());
    for entry in read_log() {
        uris.insert(entry.uri);
    }
    let strings: String = uris.into_iter().collect();
    
    let dict = b"https://artisan-ma.net/img /api/img width? context shop 2000 1000 600 400 www";
    println!("brotli  5: {}",        test_dict(&strings, &Options { brotli_level: 5, dict: b"" }));
    println!("brotli  5 + dict: {}", test_dict(&strings, &Options { brotli_level: 5, dict }));
    println!("brotli 11 + dict: {}", test_dict(&strings, &Options { brotli_level: 11, dict  }));

}


