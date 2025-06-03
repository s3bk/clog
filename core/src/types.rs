use std::collections::HashMap;
use std::marker::PhantomData;
use std::{io, net::Ipv6Addr};
use std::hash::Hash;

use anyhow::{Context, Error, anyhow};
use better_io::BetterBufRead;
use bytemuck::{bytes_of, bytes_of_mut, cast_mut, try_cast, Pod};
use bytes::Buf;
use indexmap::IndexSet;
use istring::SmallString;
use itertools::intersperse;
use pco::data_types::Number;
use pco::ChunkConfig;
use pco::{wrapped::{FileCompressor, FileDecompressor}, DeltaSpec};
use string_interner::backend::StringBackend;
use string_interner::symbol::SymbolU32;
use string_interner::{StringInterner, Symbol};

use crate::slice::{Tuple1, Tuple2};
#[cfg(feature="encode")]
use crate::DataBuilderEncode;

use crate::Input;
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

const STR_SEP_1: char = '\n';
const STR_SEP_1_STR: &str = "\n";

#[cfg(feature="encode")]
fn write_string_set_inner<'a, W: io::Write + Pos>(set: &StringInterner<StringBackend, BuildHasher>, f: &FileCompressor, mut writer: W, opt: &Options) -> Result<(u32, W), Error> {
    let strings: String = intersperse(set.iter().map(|(_, s)| s), STR_SEP_1_STR).collect();
    let len = compress_string(&mut writer, &strings, opt)?;
    Ok((len as u32, writer))
}
fn read_string_set_inner<'a, 'r>(f: &FileDecompressor, reader: Input<'r>, size: u32) -> Result<(StringInterner<StringBackend, BuildHasher>, Input<'r>), Error> {
    let (strings, reader) = decompress_string(reader, size as usize)?;
    let mut set = StringInterner::with_hasher(BuildHasher::default());
    set.extend(strings.split(STR_SEP_1));
    Ok((set, reader))
}

#[cfg(feature="encode")]
fn write_string_set<'a, W: io::Write + Pos>(set: &StringInterner<StringBackend, BuildHasher>, f: &FileCompressor, slice: &'a [u32], writer: W, opt: &Options) -> Result<(u32, W), Error> {
    let (len, writer) = write_string_set_inner(set, f, writer, opt)?;
    let writer = compress_slice(f, writer, slice, DeltaSpec::None)?;
    Ok((len as u32, writer))
}
fn read_string_set<'a, 'r>(f: &FileDecompressor, slice: &'a mut [u32], reader: Input<'r>, size: u32) -> Result<(StringInterner<StringBackend, BuildHasher>, Input<'r>), Error> {
    let (set, reader) = read_string_set_inner(f, reader, size)?;
    let reader = decompress_slice(f, reader, slice)?;
    Ok((set, reader))
}
impl DataBuilder for HashStrings {
    type CompressedItem = u32;
    type Item<'a> = &'a str;
    type Slice<'a> = &'a [u32];
    type SliceMut<'a> = &'a mut [u32];
    type Size = u32;
    type Data = Tuple1<u32>;
    
    fn add<'a>(&mut self, item: Self::Item<'a>) -> Self::CompressedItem {
        let sym = self.set.get_or_intern(item);
        sym.to_usize() as u32
    }
    fn read<'a, 'r>(f: &FileDecompressor, slice: Self::SliceMut<'a>, reader: Input<'r>, size: Self::Size) -> Result<(Self, Input<'r>), Error> {
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

#[derive(Clone)]
pub struct StringMap {
    keys: StringInterner<StringBackend, BuildHasher>,
    values: StringInterner<StringBackend, BuildHasher>,
    entries: IndexSet<Vec<(u32, u32)>, BuildHasher>,
}
impl Default for StringMap {
    fn default() -> Self {
        StringMap {
            keys: StringInterner::with_hasher(BuildHasher::default()),
            values: StringInterner::with_hasher(BuildHasher::default()),
            entries: IndexSet::with_hasher(BuildHasher::default())
        }
    }
}

/*
Encode as
 keys
 values
 entry_len[n_entries]
 key_idx[n_pairs]
 val_idx[n_pairs]
*/

impl DataBuilder for StringMap {
    type CompressedItem = u32;
    type Item<'a> = Vec<(&'a str, &'a str)>;
    type Slice<'a> = &'a [u32];
    type SliceMut<'a> = &'a mut [u32];
    type Size = (u32, u32, u32);
    type Data = Tuple1<u32>;
    
    fn add<'a>(&mut self, item: Self::Item<'a>) -> Self::CompressedItem {
        let mut entry = vec![];
        for (key, val) in item {
            let key_idx = self.keys.get_or_intern(key).to_usize() as u32;
            let val_idx = self.values.get_or_intern(val).to_usize() as u32;
            entry.push((key_idx, val_idx));
        }
        let (entry_idx, _) = self.entries.insert_full(entry);
        entry_idx as u32
    }
    fn read<'a, 'r>(f: &FileDecompressor, slice: Self::SliceMut<'a>, reader: Input<'r>, size: Self::Size) -> Result<(Self, Input<'r>), Error> {
        let (keys_size, vals_size, n_entries) = size;

        // set of key strings
        let (key_set, reader) = read_string_set_inner(f, reader, keys_size)?;

        // set of value strings
        let (val_set, reader) = read_string_set_inner(f, reader, vals_size)?;

        let mut entries_len: Vec<u16> = vec![0; n_entries as usize];

        // length of entry vecs
        let reader = decompress_slice(f, reader, &mut entries_len)?;
        let n_total: usize = entries_len.iter().map(|&n| n as usize).sum();

        let mut keys_idx: Vec<u32> = vec![0; n_total];
        // concatenated entry key indices
        let reader = decompress_slice(f, reader, &mut keys_idx)?;
        let mut val_idx: Vec<u32> = vec![0; n_total];
        // concatenated entry value indices
        let reader = decompress_slice(f, reader, &mut val_idx)?;

        let mut iter = keys_idx.into_iter().zip(val_idx);
        let mut entries = IndexSet::with_capacity_and_hasher(entries_len.len(), BuildHasher::default());
        for &entry_len in entries_len.iter() {
            let pairs: Vec<(u32, u32)> = iter.by_ref().take(entry_len as usize).collect();
            entries.insert(pairs);
        }

        let reader = decompress_slice(f, reader, slice)?;

        Ok((StringMap { keys: key_set, values: val_set, entries }, reader))
    }
    fn get<'a>(&'a self, compressed: Self::CompressedItem) -> Option<Self::Item<'a>> {
        if self.entries.len() == 0 {
            return Some(vec![]);
        }
        let entry = self.entries.get_index(compressed as usize)?;
        Some(entry.iter().filter_map(|&(key_idx, val_idx)| {
            let key = self.keys.resolve(SymbolU32::try_from_usize(key_idx as usize)?)?;
            let val = self.values.resolve(SymbolU32::try_from_usize(val_idx as usize)?)?;
            Some((key, val))
        }).collect())
    }
}

#[cfg(feature="encode")]
impl DataBuilderEncode for StringMap {
    fn write<'a, W: io::Write + Pos>(&self, f: &FileCompressor, slice: Self::Slice<'a>, writer: W, opt: &Options) -> Result<(Self::Size, W), Error> {
        // set of key strings
        let (keys_size, writer) = write_string_set_inner(&self.keys, f, writer, opt)?;
        
        // set of value strings
        let (vals_size, writer) = write_string_set_inner(&self.values, f, writer, opt)?;
        
        // length of entry vecs
        let entries_len: Vec<u16> = self.entries.iter().map(|v| v.len() as u16).collect();
        let writer = compress_slice(f, writer, &entries_len, DeltaSpec::None)?;

        let (keys_idx, vals_idx): (Vec<u32>, Vec<u32>) = self.entries.iter().flat_map(|v| v.iter().cloned()).unzip();
        // concatenated entry key indices
        let writer = compress_slice(f, writer, &keys_idx, DeltaSpec::Auto)?;
        
        // concatenated entry value indices
        let writer = compress_slice(f, writer, &vals_idx, DeltaSpec::Auto)?;
        
        let writer = compress_slice(f, writer, slice, DeltaSpec::Auto)?;

        let n_entries = self.entries.len() as u32;

        let size = (keys_size, vals_size, n_entries);
        Ok((size, writer))
    }
}

#[cfg(feature="encode")]
#[test]
fn test_stringmap() {
    let mut writer = vec![];

    let f = FileCompressor::default();
    let writer = f.write_header(writer).unwrap();

    println!("offset {}", writer.pos());
    let mut map = StringMap::default();
    let entry = vec![("Foo", "bar"), ("baz", "0123 412")];
    let n = map.add(entry.clone());
    let (size, writer) = map.write(&f, &[n], writer, &Options::default()).unwrap();

    let reader = Input::new(writer.as_slice());
    let (f, reader) = FileDecompressor::new(reader).unwrap();
    let mut slice = vec![0];
    let (map2, reader) = StringMap::read(&f, &mut slice, reader, size).unwrap();

    assert_eq!(map2.get(n).unwrap(), entry);
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
    type Data = Tuple1<u32>;

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
    fn read<'a, 'r>(f: &FileDecompressor, slice: Self::SliceMut<'a>, reader: Input<'r>, (offsets_len, cdata_len): Self::Size) -> Result<(Self, Input<'r>), Error> {
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
    type Data = Tuple1<u32>;
    
    fn add<'a>(&mut self, item: Self::Item<'a>) -> Self::CompressedItem {
        match item {
            None => 0,
            Some(item) => {
                let sym = self.set.get_or_intern(item);
                sym.to_usize() as u32
            }
        }
    }
    fn read<'a, 'r>(f: &FileDecompressor, slice: Self::SliceMut<'a>, reader: Input<'r>, size: Self::Size) -> Result<(Self, Input<'r>), Error> {
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
    type Data = Tuple2<u32, u32>;

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
    fn read<'a, 'r>(f: &FileDecompressor, (prefixes, suffixes): Self::SliceMut<'a>, reader: Input<'r>, size: Self::Size) -> Result<(Self, Input<'r>), Error> {
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
    type Data = Tuple1<N>;

    fn add<'a>(&mut self, item: Self::Item<'a>) -> Self::CompressedItem {
        item
    }
    fn read<'a, 'r>(f: &FileDecompressor, slice: Self::SliceMut<'a>, reader: Input<'r>, size: Self::Size) -> Result<(Self, Input<'r>), Error> {
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
    type Data = Tuple1<u32>;

    fn add<'a>(&mut self, item: Self::Item<'a>) -> Self::CompressedItem {
        if self.offset == 0 {
            self.offset = if item != 0 { item } else { 1 };
        }
        item.wrapping_sub(self.offset) as u32
    }
    fn read<'a, 'r>(f: &FileDecompressor, slice: Self::SliceMut<'a>, mut reader: Input<'r>, size: Self::Size) -> Result<(Self, Input<'r>), Error> {
        let mut offset = 0;
        let dest_bytes = bytes_of_mut(&mut offset);
        let bytes = reader.take_n(dest_bytes.len())?;
        dest_bytes.copy_from_slice(bytes);
        let reader = decompress_slice(f, reader, slice)?;
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

fn decompress_string(reader: Input, len: usize) -> Result<(String, Input), Error> {
    let (buffer, rest) = decompress_data(reader, len)?;
    let buffer = String::from_utf8(buffer)?;
    Ok((buffer, rest))
}
fn decompress_data(mut reader:Input, len: usize) -> Result<(Vec<u8>, Input), Error> {
    use brotli_decompressor::BrotliDecompress;

    // println!("read Brotli strings at {}", reader.pos());

    let mut input = reader.take_n(len)?;
    let mut buffer: Vec<u8> = vec![];
    BrotliDecompress(
        &mut input,
        &mut buffer,
    )?;
    Ok((buffer, reader))
}

fn compress_slice<'a, T: Number, W: io::Write + Pos>(f: &FileCompressor, writer: W, slice: &'a [T], delta_spec: DeltaSpec) -> Result<W, Error> {
    // println!("write [{}] at {}", type_name::<T>(), writer.pos());
    if slice.len() == 0 {
        return Ok(writer);
    }

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

fn decompress_slice<'a, 'r, T: Number>(f: &FileDecompressor, reader: Input<'r>, slice: &'a mut [T]) -> Result<Input<'r>, Error> {
    println!("read [{}; {}] at {}", std::any::type_name::<T>(), slice.len(), reader.pos());
    if slice.len() == 0 {
        return Ok(reader);
    }

    let reader_clone = reader.clone();
    let (decompressor, reader) = f.chunk_decompressor(reader).context("chunk header")?;
    let mut page = decompressor.page_decompressor(reader, slice.len()).context("page")?;
    let progress = page.decompress(slice).context("decompress")?;
    assert!(progress.finished);
    assert_eq!(progress.n_processed, slice.len());
    
    Ok(page.into_src())
}
