use bytes::BytesMut;
use paste::paste;
use postcard::take_from_bytes;
use soa_rs::{Soa, Soars};
use std::io::{self, Cursor};
use std::ops::Range;
use pco::wrapped::{FileCompressor, FileDecompressor};
use anyhow::Error;
use better_io::BetterBufRead;
use serde::{Serialize, Deserialize};

use crate::{types::{HashIpv6, HashStrings, HashStringsOpt, NumberSeries, TimeSeries}, util::ReadAdapter, DataBuilder, Options, Pos, RequestEntry};

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
            pub fn write<'a, W: io::Write + Pos>(&self, f: &FileCompressor, writer: W, opt: &Options) -> Result<([< $builder Sizes >], W), Error> {
                $(
                    $( 
                        // println!("write {} at {}", stringify!($field), writer.pos());
                        let ($field, writer) = self.$field.write(f, self.soa.$field(), writer, opt)?;
                    )?
                    $(
                        // println!("write {} at {}", stringify!($field2), writer.pos());
                        let ($field2, writer) = self.$field2.write(f, ( $( self.soa.$part() ),* ), writer, opt)?;
                    )?
                )*
                let s = [< $builder Sizes >] {
                    rows: self.soa.len() as u32,
                    $(
                        $( $field )?
                        $( $field2 )?
                    ,)*
                };
                Ok((s, writer))
            }
            pub fn read<'a, R: BetterBufRead + Pos>(f: &FileDecompressor, reader: R, size: [< $builder Sizes >]) -> Result<(Self, R), Error> {
                let mut soa = Soa::<$compressed>::default();
                soa.reserve(size.rows as usize);
                soa.extend(std::iter::repeat(Default::default()).take(size.rows as usize));

                let [<  $compressed SlicesMut >] {
                    $( $( $field, )? $( $( $part, )* )? )*
                } = soa.slices_mut();
                $(
                    $(
                        // println!("read {} at {}", stringify!($field), reader.pos());
                        let ($field, reader) = <$type as DataBuilder>::read(f, $field, reader, size.$field)?;
                    )?
                    $(
                        // println!("read {} at {}", stringify!($field2), reader.pos());
                        let ($field2, reader) = <$type2 as DataBuilder>::read(
                            f,
                            ( $( $part ),* ),
                            reader,
                            size.$field2
                        )?;
                    )?
                )*
                
                Ok(($builder {
                    soa,
                    $(
                        $( $field, )?
                        $( $field2, )*
                    )*
                }, reader))
            }
        }
        );
    };
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
    pub fn write_to(&self, writer: BytesMut, opt: &Options) -> BytesMut {
        let f = FileCompressor::default();
        let buf = Vec::with_capacity(10 * self.len() + 100);
        let buf = f.write_header(buf).unwrap();
        let (sizes, buf) = self.write(&f, buf, opt).unwrap();
        let mut writer = postcard::to_extend(&sizes, writer).unwrap();
        writer.extend_from_slice(&buf);
        writer
    }
    pub fn from_slice(data: &[u8]) -> Result<Self, Error> {
        let (size, rest) = take_from_bytes(data)?;
        let reader = ReadAdapter::new(rest);
        let (f, reader) = FileDecompressor::new(reader)?;
        let (builder, reader) = Builder::read(&f, reader, size)?;
        Ok(builder)
    }
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
