use bytes::BytesMut;
use paste::paste;
use serde::de::DeserializeOwned;
use soa_rs::{Soa, Soars};
use core::slice;
use std::alloc::Layout;
use std::io::{self, Cursor};
use std::ops::Range;
use pco::wrapped::{FileCompressor, FileDecompressor};
use anyhow::{bail, Error};
use better_io::BetterBufRead;
use serde::{Serialize, Deserialize};

use crate::types::DataSeries;
use crate::util::WriteAdapter;
use crate::{types::{HashIpv6, HashStrings, HashStringsOpt, NumberSeries, TimeSeries, StringMap}, util::ReadAdapter, DataBuilder, Options, Pos, RequestEntry, 
    slice::{SliceTrait, Owned},
    Input
};
use crate as clog;

#[cfg(feature="encode")]
use crate::DataBuilderEncode;


#[derive(Serialize, Deserialize, Debug)]
struct Header {
    version: u32,
    len: u32,
}

const V2: u32 = 2;
const V3: u32 = 3;
const V4: u32 = 3;
const SHEMA_VERSION: u32 = V4;

#[derive(clog_derive::Shema)]
pub struct ShemaImpl {
    status: NumberSeries<u16>,
    method: HashStrings,
    uri: HashStrings,
    #[clog(max_version=V2)]
    ua: HashStringsOpt,
    #[clog(max_version=V2)]
    referer: HashStringsOpt,
    ip: HashIpv6,
    port: NumberSeries<u16>,
    time: TimeSeries,
    #[clog(min_version=V2)]
    body: DataSeries,
    #[clog(min_version=V3)]
    headers: StringMap,
    #[clog(min_version=V3)]
    host: HashStrings,
    #[clog(min_version=V4)]
    proto: NumberSeries<u16>,
}

pub type BatchEntry<'a> = ShemaImplItem<'a>;
pub type Builder = ShemaImplBuilder;

pub fn decode<'a, T: DeserializeOwned>(mut input: Input<'a>) -> Result<(T, Input<'a>), Error> {
    let (val, rest) = postcard::take_from_bytes(&input)?;
    input.advance(input.len() - rest.len());
    Ok((val, input))
}
pub fn encode<T: Serialize, W: Extend<u8>>(val: T, writer: W) -> Result<W, Error> {
    let writer = postcard::to_extend(&val, writer)?;
    Ok(writer)
}

pub trait Shema: Sized {
    type Item<'a>;
    type Fields: SliceTrait;
    
    fn with_capacity(n: usize) -> Self;

    fn add(&mut self, item: Self::Item<'_>);
    fn get(&self, idx: usize) -> Option<Self::Item<'_>>;
    
    fn decompress(&self, c: <Self::Fields as SliceTrait>::Elem) -> Self::Item<'_>;
    fn fields(&self) -> &Owned<Self::Fields>;

    #[cfg(feature="encode")]
    fn write(&self, f: &FileCompressor, writer: BytesMut, opt: &Options, version: u32) -> Result<BytesMut, Error>;
    fn read<'a>(f: &FileDecompressor, data: Input<'a>, len: usize, version: u32) -> Result<(Self, Input<'a>), Error>;
    fn reserve(&mut self, additional: usize);

    fn iter(&self) -> impl Iterator<Item=Self::Item<'_>> + ExactSizeIterator {
        self.fields().iter().map(|i| self.decompress(i))
    }
    fn range(&self, range: Range<usize>) -> impl Iterator<Item=Self::Item<'_>> + ExactSizeIterator + DoubleEndedIterator + '_ {
        self.fields().iter().skip(range.start).take(range.end - range.start).map(|i| self.decompress(i))
    }

    #[cfg(feature="encode")]
    fn write_to(&self, mut writer: BytesMut, opt: &Options) -> BytesMut {
        let f = FileCompressor::default();
        writer.reserve(10 * self.len() + 100);

        let header = Header {
            version: SHEMA_VERSION,
            len: self.len() as u32,
        };
        let writer = postcard::to_extend(&header, writer).unwrap();
        let writer = WriteAdapter(writer);
        let WriteAdapter(writer) = f.write_header(writer).unwrap();
        let writer = self.write(&f, writer, opt, SHEMA_VERSION).unwrap();
        writer
    }
    fn from_slice(data: &[u8]) -> Result<Self, Error> {
        let input = Input::new(data);
        let (header, reader) = decode::<Header>(input)?;
        println!("header: {header:?}");
        if header.version > SHEMA_VERSION {
            bail!("found version {} but compiled with version {}", header.version, SHEMA_VERSION);
        }
        println!("after header reader at {}", reader.pos());
        let (f, reader) = FileDecompressor::new(reader)?;
        println!("after decmpressor reader at {}", reader.pos());
        let (builder, reader) = Self::read(&f, reader, header.len as usize, header.version)?;
        Ok(builder)
    }
    #[cfg(feature="encode")]
    fn to_vec(&self, options: &Options) -> Vec<u8> {
        let buf = BytesMut::new();
        let buf = self.write_to(buf, options);
        buf.to_vec()
    }
    fn len(&self) -> usize {
        self.fields().len()
    }
}

impl<'a> From<&'a RequestEntry> for BatchEntry<'a> {
    fn from(e: &'a RequestEntry) -> Self {
        let ip = match e.ip {
            std::net::IpAddr::V4(ip) => ip.to_ipv6_mapped(),
            std::net::IpAddr::V6(ip) => ip
        };
        BatchEntry {
            status: e.status,
            method: &e.method,
            uri: &e.uri,
            ua: None,
            referer: None,
            ip,
            port: e.port,
            time: e.time,
            body: e.body.as_deref(),
            headers: e.headers.split(),
            host: &e.host,
            proto: e.proto as u16
        }
    }
}
