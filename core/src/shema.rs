use bytes::BytesMut;
use paste::paste;
use postcard::take_from_bytes;
use soa_rs::{Soa, Soars};
use std::io::{self, Cursor};
use std::ops::Range;
use pco::wrapped::{FileCompressor, FileDecompressor};
use anyhow::{bail, Error};
use better_io::BetterBufRead;
use serde::{Serialize, Deserialize};

use crate::util::WriteAdapter;
use crate::{types::{HashIpv6, HashStrings, HashStringsOpt, NumberSeries, TimeSeries}, util::ReadAdapter, DataBuilder, Options, Pos, RequestEntry};

#[cfg(feature="encode")]
use crate::DataBuilderEncode;

macro_rules! define_type {
    (struct $builder:ident { $( $($field:ident : $type:ty )? $(> $field2:ident : $type2:ty = ( $( $part:ident: $t:ty ,)* ) )? ,)* }, item $item:ident, compressed $compressed:ident ) => {
        paste!(
        #[derive(Default, Clone)]
        pub struct $builder {
            $(
                $( $field: $type )?
                $( $field2: $type2 )?
            ,)*
            soa: Soa<$compressed>
        }

        #[derive(Debug, Clone, Serialize, Deserialize)]
        #[serde(bound(deserialize = "'de: 'a"))]
        pub struct $item<'a> {
            $(
                $( pub $field: <$type as DataBuilder>::Item<'a> )?
                $( pub $field2: <$type2 as DataBuilder>::Item<'a> )?
            ,)*
        }
        
        #[derive(Soars, PartialEq, Debug, Copy, Clone, Default)]
        pub struct $compressed {
            $(
                $( $field: <$type as DataBuilder>::CompressedItem )?
                $( $( $part: $t ),* )?
            ,)*
        }

        #[derive(Serialize, Deserialize, Debug, PartialEq, Copy, Clone)]
        pub struct [< $builder Sizes >] {
            rows: u32,
            $(
                $( $field: <$type as DataBuilder>::Size )?
                $( $field2: <$type2 as DataBuilder>::Size )?
            ,)*
        }

        impl $builder {
            pub fn add(&mut self, item: $item) {
                $(
                    $( let $field = self.$field.add(item.$field); )?
                    $( 
                        let ( $( $part ,)* ) = self.$field2.add(item.$field2);
                    )?
                )*
                let compressed = $compressed {
                    $(
                        $( $field, )?
                        $( 
                            $( $part, )*
                        )?
                    )*
                };
                self.soa.push(compressed);
            }
            pub fn get(&self, idx: usize) -> Option<$item> {
                let [< $compressed Ref >] {
                    $( $( $field, )? $( $( $part, )* )? )*
                } = self.soa.get(idx)?;
                Some($item {
                    $(
                        $( $field: self.$field.get(*$field)?, )?
                        $( 
                            $field2: self.$field2.get(( $( *$part ,)* ))?,
                        )?
                    )*
                })
            }
            fn decompress(&self, c: [< $compressed Ref >]) -> $item {
                let [< $compressed Ref >] {
                    $( $( $field, )? $( $( $part, )* )? )*
                } = c;
                $item {
                    $(
                        $( $field: self.$field.get(*$field).expect(stringify!($field)), )?
                        $( 
                            $field2: self.$field2.get(( $( *$part ,)* )).expect(stringify!($field2)),
                        )?
                    )*
                }
            }
            pub fn iter(&self) -> impl Iterator<Item=$item> + ExactSizeIterator + '_ {
                self.soa.iter().map(|i| self.decompress(i))
            }
            pub fn range(&self, range: Range<usize>) -> impl Iterator<Item=$item> + ExactSizeIterator + DoubleEndedIterator + '_ {
                self.soa.iter().skip(range.start).take(range.end - range.start).map(|i| self.decompress(i))
            }
            #[cfg(feature="encode")]
            pub fn write(&self, f: &FileCompressor, writer: BytesMut, opt: &Options) -> Result<BytesMut, Error> {
                let scratch = Vec::with_capacity(8 * self.soa.len() + 100);
                $(
                    $( 
                        //println!("write {}", stringify!($field));
                        let ($field, mut scratch) = self.$field.write(f, self.soa.$field(), scratch, opt)?;
                    )?
                    $(
                        //println!("write {}", stringify!($field2));
                        let ($field2, mut scratch) = self.$field2.write(f, ( $( self.soa.$part() ),* ), scratch, opt)?;
                    )?
                    let field_size = $( $field )? $( $field2 )?;

                    //println!("at {}", writer.len());
                    let mut writer = postcard::to_extend(&field_size, writer)?;
                    //println!("data at {}", writer.len());
                    writer.extend_from_slice(&scratch);
                    scratch.clear();
                )*
                Ok(writer)
            }
            pub fn read<'a>(f: &FileDecompressor, data: &'a [u8], len: usize) -> Result<(Self, &'a [u8]), Error> {
                // let start = data.as_ptr() as usize;
                // let pos = |d: &[u8]| d.as_ptr() as usize - start;

                let mut soa = Soa::<$compressed>::default();
                soa.reserve(len as usize);
                soa.extend(std::iter::repeat(Default::default()).take(len as usize));

                let [<  $compressed SlicesMut >] {
                    $( $( $field, )? $( $( $part, )* )? )*
                } = soa.slices_mut();
                $(
                    //println!("field header at {}", pos(data));
                    let (field_size, data) = take_from_bytes(data)?;
                    $(
                        //println!("read {} at {}", stringify!($field), pos(data));
                        let ($field, data) = <$type as DataBuilder>::read(f, $field, data, field_size)?;
                    )?
                    $(
                        //println!("read {} at {}", stringify!($field2), pos(data));
                        let ($field2, data) = <$type2 as DataBuilder>::read(
                            f,
                            ( $( $part ),* ),
                            data,
                            field_size
                        )?;
                    )?
                )*
                
                Ok(($builder {
                    soa,
                    $(
                        $( $field, )?
                        $( $field2, )*
                    )*
                }, data))
            }
        }
        );
    };
}

#[derive(Serialize, Deserialize)]
struct Header {
    version: u32,
    len: usize,
}

define_type!(
struct Builder {
    status: NumberSeries<u16>,
    method: HashStrings,
    uri: HashStrings,
    ua: HashStringsOpt,
    referer: HashStringsOpt,
    > ip: HashIpv6 = (ip_pre_idx: u32, ip_suffix: u32,),
    port: NumberSeries<u16>,
    time: TimeSeries,
}, item BatchEntry, compressed CompressedEntry);

impl Builder {
    #[cfg(feature="encode")]
    pub fn write_to(&self, mut writer: BytesMut, opt: &Options) -> BytesMut {
        let f = FileCompressor::default();
        writer.reserve(10 * self.len() + 100);

        let header = Header {
            version: 1,
            len: self.soa.len()
        };
        let writer = postcard::to_extend(&header, writer).unwrap();
        let writer = WriteAdapter(writer);
        let WriteAdapter(writer) = f.write_header(writer).unwrap();
        let writer = self.write(&f, writer, opt).unwrap();
        writer
    }
    pub fn from_slice(data: &[u8]) -> Result<Self, Error> {
        let (header, reader) = take_from_bytes::<Header>(data)?;
        if header.version != 1 {
            bail!("Version mismatch {} != 1", header.version);
        }
        let (f, reader) = FileDecompressor::new(reader)?;
        let (builder, reader) = Builder::read(&f, reader, header.len)?;
        Ok(builder)
    }
    #[cfg(feature="encode")]
    pub fn to_vec(&self, options: &Options) -> Vec<u8> {
        let buf = BytesMut::new();
        let buf = self.write_to(buf, options);
        buf.to_vec()
    }
    pub fn reserve(&mut self, additional: usize) {
        self.soa.reserve(additional);
    }
    pub fn len(&self) -> usize {
        self.soa.len()
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
            ua: e.user_agent.as_deref(),
            referer: e.referer.as_deref(),
            ip,
            port: e.port,
            time: e.time
        }
    }
}
