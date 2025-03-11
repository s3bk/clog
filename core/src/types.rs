use std::marker::PhantomData;
use std::{io, net::Ipv6Addr};
use std::hash::Hash;

use anyhow::{Context, Error};
use better_io::BetterBufRead;
use bytemuck::bytes_of_mut;
use indexmap::IndexSet;
use itertools::intersperse;
use pco::data_types::Number;
use pco::ChunkConfig;
use pco::{wrapped::{FileCompressor, FileDecompressor}, DeltaSpec};
use string_interner::backend::StringBackend;
use string_interner::symbol::SymbolU32;
use string_interner::{StringInterner, Symbol};

#[cfg(feature="encode")]
use crate::DataBuilderEncode;

use crate::{util::BrotliReadAdapter, DataBuilder, Options, Pos, BuildHasher};


#[derive(Clone)]
pub struct HashStrings {
    set: StringInterner<StringBackend, BuildHasher>
}
impl Default for HashStrings {
    fn default() -> Self {
        HashStrings { set: StringInterner::with_hasher(BuildHasher::default()) }
    }
}
#[cfg(feature="encode")]
fn write_string_set<'a, W: io::Write + Pos>(set: &StringInterner<StringBackend, BuildHasher>, f: &FileCompressor, slice: &'a [u32], mut writer: W, opt: &Options) -> Result<(u32, W), Error> {
    let strings: String = intersperse(set.iter().map(|(_, s)| s), "\n").collect();
    let len = compress_string(&mut writer, &strings, opt)?;
    let writer = compress_slice(f, writer, slice, DeltaSpec::None)?;
    Ok((len as u32, writer))
}
fn read_string_set<'a, 'r>(f: &FileDecompressor, slice: &'a mut [u32], reader: &'r [u8], size: u32) -> Result<(StringInterner<StringBackend, BuildHasher>, &'r [u8]), Error> {
    let (strings, reader) = decompress_string(reader, size as usize)?;
    let mut set = StringInterner::with_hasher(BuildHasher::default());
    set.extend(strings.split("\n"));
    let reader = decompress_slice(f, reader, slice)?;
    Ok((set, reader))
}
impl DataBuilder for HashStrings {
    type CompressedItem = u32;
    type Item<'a> = &'a str;
    type Slice<'a> = &'a [u32];
    type SliceMut<'a> = &'a mut [u32];
    type Size = u32;
    
    fn add<'a>(&mut self, item: Self::Item<'a>) -> Self::CompressedItem {
        let sym = self.set.get_or_intern(item);
        sym.to_usize() as u32
    }
    fn read<'a, 'r>(f: &FileDecompressor, slice: Self::SliceMut<'a>, reader: &'r [u8], size: Self::Size) -> Result<(Self, &'r [u8]), Error> {
        let (set, reader) = read_string_set(f, slice, reader, size)?;
        Ok((HashStrings { set }, reader))
    }
    fn get<'a>(&'a self, compressed: Self::CompressedItem) -> Option<Self::Item<'a>> {
        self.set.resolve(SymbolU32::try_from_usize(compressed as usize)?)
    }
}
#[cfg(feature="encode")]
impl DataBuilderEncode for HashStrings {
    fn write<'a, W: io::Write + Pos>(&self, f: &FileCompressor, slice: Self::Slice<'a>, writer: W, opt: &Options) -> Result<(Self::Size, W), Error> {
        write_string_set(&self.set, f, &slice, writer, opt)
    }
}

#[derive(Default, Clone)]
pub struct DataSeries {
    data: Vec<u8>,
    offsets: Vec<u32>,
}
impl DataBuilder for DataSeries {
    type CompressedItem = u32;
    type Item<'a> = Option<&'a [u8]>;
    type Slice<'a> = &'a [u32];
    type SliceMut<'a> = &'a mut [u32];
    type Size = (u32, u32); // offsets len, compressed data len

    fn add<'a>(&mut self, item: Self::Item<'a>) -> Self::CompressedItem {
        if let Some(data) = item {
            self.data.extend_from_slice(data);
            self.offsets.push(self.data.len() as u32);
            self.offsets.len() as u32
        } else {
            0
        }
    }
    fn get<'a>(&'a self, compressed: Self::CompressedItem) -> Option<Self::Item<'a>> {
        if compressed == 0 {
            Some(None)
        } else {
            let i = compressed as usize;
            let start = if i == 1 { 0 } else {
                *self.offsets.get(i - 2)? as usize
            };
            let end = *self.offsets.get(i - 1)? as usize;
            Some(Some(self.data.get(start .. end)?))
        }
    }
    fn read<'a, 'r>(f: &FileDecompressor, slice: Self::SliceMut<'a>, reader: &'r [u8], (offsets_len, cdata_len): Self::Size) -> Result<(Self, &'r [u8]), Error> {
        let mut offsets = vec![0; offsets_len as usize];
        let mut reader = decompress_slice(f, reader, slice)?;
        if offsets_len > 0 {
            reader = decompress_slice(f, reader, &mut offsets)?;
        }
        let (data, reader) = decompress_data(reader, cdata_len as usize)?;
        Ok((DataSeries {
            data, offsets
        }, reader))
    }
}

#[cfg(feature="encode")]
impl DataBuilderEncode for DataSeries {
    fn write<'a, W: io::Write + Pos>(&self, f: &FileCompressor, slice: Self::Slice<'a>, writer: W, opt: &Options) -> Result<(Self::Size, W), Error> {
        let mut writer = compress_slice(f, writer, slice, DeltaSpec::TryLookback)?;
        if self.offsets.len() > 0 {
            writer = compress_slice(f, writer, &self.offsets, DeltaSpec::TryConsecutive(2))?;
        }
        let cdata_len = compress_data(&mut writer, &self.data, opt)? as u32;
        Ok((((self.offsets.len() as u32, cdata_len)), writer))
    }
}

#[derive(Clone)]
pub struct HashStringsOpt {
    set: StringInterner<StringBackend, BuildHasher>
}
impl Default for HashStringsOpt {
    fn default() -> Self {
        HashStringsOpt { set: StringInterner::with_hasher(BuildHasher::default()) }
    }
}
impl DataBuilder for HashStringsOpt {
    type CompressedItem = u32;
    type Item<'a> = Option<&'a str>;
    type Slice<'a> = &'a [u32];
    type SliceMut<'a> = &'a mut [u32];
    type Size = u32;
    
    fn add<'a>(&mut self, item: Self::Item<'a>) -> Self::CompressedItem {
        match item {
            None => 0,
            Some(item) => {
                let sym = self.set.get_or_intern(item);
                sym.to_usize() as u32
            }
        }
    }
    fn read<'a, 'r>(f: &FileDecompressor, slice: Self::SliceMut<'a>, reader: &'r [u8], size: Self::Size) -> Result<(Self, &'r [u8]), Error> {
        let (set, reader) = read_string_set(f, slice, reader, size)?;
        Ok((HashStringsOpt { set }, reader))
    }
    fn get<'a>(&'a self, compressed: Self::CompressedItem) -> Option<Self::Item<'a>> {
        match compressed {
            0 => Some(None),
            i => Some(self.set.resolve(SymbolU32::try_from_usize(i as usize - 1)?.clone()))
        }
    }
}
#[cfg(feature="encode")]
impl DataBuilderEncode for HashStringsOpt {
    #[cfg(feature="encode")]
    fn write<'a, W: io::Write + Pos>(&self, f: &FileCompressor, slice: Self::Slice<'a>, writer: W, opt: &Options) -> Result<(Self::Size, W), Error> {
        write_string_set(&self.set, f, &slice, writer, opt)
    }
}

fn copy_to(reader: &mut impl BetterBufRead, mut out: &mut [u8]) -> Result<(), Error> {
    while out.len() > 0 {
        let max = out.len().min(reader.capacity().unwrap_or(usize::MAX));
        reader.fill_or_eof(max)?;

        let buf = reader.buffer();
        let n = buf.len().min(out.len());
        let (head, tail) = out.split_at_mut(n);
        head.copy_from_slice(&buf[..n]);
        out = tail;

        reader.consume(n);
    }
    Ok(())
}


#[derive(Default, Clone)]
pub struct HashIpv6 {
    prefixes: IndexSet<[u32; 3], BuildHasher>,
}
impl DataBuilder for HashIpv6 {
    type Item<'a> = Ipv6Addr;
    type CompressedItem = (u32, u32);
    type Slice<'a> = (&'a [u32], &'a [u32]);
    type SliceMut<'a> = (&'a mut [u32], &'a mut [u32]);
    type Size = u32;

    fn add<'a>(&mut self, item: Self::Item<'a>) -> Self::CompressedItem {
        let bits = item.to_bits();
        let prefix = [
            (bits >> 96) as u32,
            (bits >> 64) as u32,
            (bits >> 32) as u32
        ];
        let suffix = bits as u32;
        let (prefix_idx, _) = self.prefixes.insert_full(prefix);
        (prefix_idx as u32, suffix)
    }
    fn read<'a, 'r>(f: &FileDecompressor, (prefixes, suffixes): Self::SliceMut<'a>, reader: &'r [u8], size: Self::Size) -> Result<(Self, &'r [u8]), Error> {
        let reader = decompress_slice(f, reader, prefixes)?;
        let mut reader = decompress_slice(f, reader, suffixes)?;

        let mut prefixes = IndexSet::with_capacity_and_hasher(size as usize, BuildHasher::default());
        for _ in 0 .. size {
            let mut val = [0; 3];
            copy_to(&mut reader, bytes_of_mut(&mut val))?;
            prefixes.insert(val);
        }

        Ok((HashIpv6 { prefixes }, reader))
    }
    fn get<'a>(&'a self, (prefix_idx, suffix): Self::CompressedItem) -> Option<Self::Item<'a>> {
        let prefix = self.prefixes.get_index(prefix_idx as usize)?;
        let bits = (prefix[0] as u128) << 96 | (prefix[1] as u128) << 64 | (prefix[2] as u128) << 32 | suffix as u128;
        Some(Ipv6Addr::from_bits(bits))
    }
}
#[cfg(feature="encode")]
impl DataBuilderEncode for HashIpv6 {
    fn write<'a, W: io::Write + Pos>(&self, f: &FileCompressor, (prefixes, suffixes): Self::Slice<'a>, writer: W, _opt: &Options) -> Result<(Self::Size, W), Error> {
        let writer = compress_slice(f, writer, prefixes, DeltaSpec::TryLookback)?;
        let mut writer = compress_slice(f, writer, suffixes, DeltaSpec::TryLookback)?;
        
        for i in self.prefixes.iter() {
            writer.write_all(bytemuck::bytes_of(i))?;
        }
        Ok((self.prefixes.len() as u32, writer))
    }
}

#[derive(Clone)]
pub struct NumberSeries<N> {
    _m: PhantomData<N>
}
impl<N> Default for NumberSeries<N> {
    fn default() -> Self {
        NumberSeries { _m: PhantomData }
    }
}
impl<N: Number> DataBuilder for NumberSeries<N> {
    type Item<'a> = N;
    type CompressedItem = N;
    type Slice<'a> = &'a [N];
    type SliceMut<'a> = &'a mut [N];
    type Size = ();

    fn add<'a>(&mut self, item: Self::Item<'a>) -> Self::CompressedItem {
        item
    }
    fn read<'a, 'r>(f: &FileDecompressor, slice: Self::SliceMut<'a>, reader: &'r [u8], size: Self::Size) -> Result<(Self, &'r [u8]), Error> {
        let reader = decompress_slice(f, reader, slice)?;
        Ok((NumberSeries { _m: PhantomData }, reader))
    }
    fn get<'a>(&'a self, compressed: Self::CompressedItem) -> Option<Self::Item<'a>> {
        Some(compressed)
    }
}
#[cfg(feature="encode")]
impl<N: Number> DataBuilderEncode for NumberSeries<N> {
    fn write<'a, W: io::Write + Pos>(&self, f: &FileCompressor, slice: Self::Slice<'a>, writer: W, _opt: &Options) -> Result<(Self::Size, W), Error> {
        let writer = compress_slice(f, writer, slice, DeltaSpec::Auto)?;
        Ok(((), writer))
    }
}

#[derive(Default, Clone)]
pub struct TimeSeries {
    offset: u64,
}
impl DataBuilder for TimeSeries {
    type CompressedItem = u32;
    type Item<'a> = u64;
    type Slice<'a> = &'a [u32];
    type SliceMut<'a> = &'a mut [u32];
    type Size = ();

    fn add<'a>(&mut self, item: Self::Item<'a>) -> Self::CompressedItem {
        if self.offset == 0 {
            self.offset = if item != 0 { item } else { 1 };
        }
        item.wrapping_sub(self.offset) as u32
    }
    fn read<'a, 'r>(f: &FileDecompressor, slice: Self::SliceMut<'a>, reader: &'r [u8], size: Self::Size) -> Result<(Self, &'r [u8]), Error> {
        let mut offset = 0;
        let dest_bytes = bytes_of_mut(&mut offset);
        let (bytes, rest) = reader.split_at(dest_bytes.len());
        dest_bytes.copy_from_slice(bytes);
        let reader = decompress_slice(f, rest, slice)?;
        Ok((TimeSeries { offset}, reader))
    }
    fn get<'a>(&'a self, compressed: Self::CompressedItem) -> Option<Self::Item<'a>> {
        Some(self.offset.wrapping_add(compressed as u64))
    }
}

#[cfg(feature="encode")]
impl DataBuilderEncode for TimeSeries {
    fn write<'a, W: io::Write + Pos>(&self, f: &FileCompressor, slice: Self::Slice<'a>, mut writer: W, opt: &Options) -> Result<(Self::Size, W), Error> {
        writer.write_all(bytemuck::bytes_of(&self.offset))?;
        let writer = compress_slice(f, writer, slice, DeltaSpec::TryConsecutive(1))?;
        Ok(((), writer))
    }
}

#[cfg(feature="encode")]
pub fn compress_string<W: io::Write + Pos>(writer: &mut W, strings: &str, opt: &Options) -> Result<usize, Error> {
    compress_data(writer, strings.as_bytes(), opt)
}
#[cfg(feature="encode")]
pub fn compress_data<W: io::Write + Pos>(writer: &mut W, mut data: &[u8], opt: &Options) -> Result<usize, Error> {
    use brotli::{enc::BrotliEncoderParams, BrotliCompress};

    // println!("write Brotli strings at {}", writer.pos());

    let mut params = BrotliEncoderParams::default();
    params.quality = opt.brotli_level as i32;

    let written = BrotliCompress(
        &mut data,
        writer,
        &params
    ).unwrap();
    Ok(written)
}

fn decompress_string(reader: &[u8], len: usize) -> Result<(String, &[u8]), Error> {
    let (buffer, rest) = decompress_data(reader, len)?;
    let buffer = String::from_utf8(buffer)?;
    Ok((buffer, rest))
}
fn decompress_data(reader: &[u8], len: usize) -> Result<(Vec<u8>, &[u8]), Error> {
    use brotli_decompressor::BrotliDecompress;

    // println!("read Brotli strings at {}", reader.pos());

    let (mut input, rest) = reader.split_at_checked(len).ok_or_else(|| anyhow::anyhow!("not enough input data"))?;
    let mut buffer: Vec<u8> = vec![];
    BrotliDecompress(
        &mut input,
        &mut buffer,
    )?;
    Ok((buffer, rest))
}

fn compress_slice<'a, T: Number, W: io::Write + Pos>(f: &FileCompressor, writer: W, slice: &'a [T], delta_spec: DeltaSpec) -> Result<W, Error> {
    // println!("write [{}] at {}", type_name::<T>(), writer.pos());

    let config = ChunkConfig::default()
        .with_compression_level(8)
        .with_delta_spec(delta_spec)
        .with_mode_spec(pco::ModeSpec::Classic)
        .with_paging_spec(pco::PagingSpec::EqualPagesUpTo(slice.len()));
    
    let time = f.chunk_compressor(slice, &config)?;
    let writer = time.write_chunk_meta(writer)?;
    let writer = time.write_page(0, writer)?;
    Ok(writer)
}

fn decompress_slice<'a, 'r, T: Number>(f: &FileDecompressor, reader: &'r [u8], slice: &'a mut [T]) -> Result<&'r [u8], Error> {
    // println!("read [{}] at {}", type_name::<T>(), reader.pos());

    let (decompressor, reader) = f.chunk_decompressor(reader).context("chunk header")?;
    let mut page = decompressor.page_decompressor(reader, slice.len()).context("page")?;
    let progress = page.decompress(slice).context("decompress")?;
    assert!(progress.finished);
    assert_eq!(progress.n_processed, slice.len());
    
    Ok(page.into_src())
}
