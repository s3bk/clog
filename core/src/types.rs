use std::marker::PhantomData;
use std::{io, net::Ipv6Addr};
use std::hash::Hash;

use anyhow::{Context, Error};
use better_io::BetterBufRead;
use brotli::enc::{BrotliEncoderParams, StandardAlloc};
use brotli::{interface, InputReferenceMut, IoReaderWrapper, IoWriterWrapper};
use bytemuck::bytes_of_mut;
use indexmap::IndexSet;
use itertools::intersperse;
use pco::data_types::Number;
use pco::ChunkConfig;
use pco::{wrapped::{FileCompressor, FileDecompressor}, DeltaSpec};
use string_interner::backend::StringBackend;
use string_interner::symbol::SymbolU32;
use string_interner::{StringInterner, Symbol};

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
fn write_string_set<'a, W: io::Write + Pos>(set: &StringInterner<StringBackend, BuildHasher>, f: &FileCompressor, slice: &'a [u32], mut writer: W, opt: &Options) -> Result<(u32, W), Error> {
    let strings: String = intersperse(set.iter().map(|(_, s)| s), "\n").collect();
    let len = compress_string(&mut writer, &strings, opt)?;
    let writer = compress_slice(f, writer, slice, DeltaSpec::None)?;
    Ok((len as u32, writer))
}
fn read_string_set<'a, R: BetterBufRead + Pos>(f: &FileDecompressor, slice: &'a mut [u32], reader: R, size: u32) -> Result<(StringInterner<StringBackend, BuildHasher>, R), Error> {
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
    fn write<'a, W: io::Write + Pos>(&self, f: &FileCompressor, slice: Self::Slice<'a>, writer: W, opt: &Options) -> Result<(Self::Size, W), Error> {
        write_string_set(&self.set, f, &slice, writer, opt)
    }
    fn read<'a, R: BetterBufRead + Pos>(f: &FileDecompressor, slice: Self::SliceMut<'a>, reader: R, size: Self::Size) -> Result<(Self, R), Error> {
        let (set, reader) = read_string_set(f, slice, reader, size)?;
        Ok((HashStrings { set }, reader))
    }
    fn get<'a>(&'a self, compressed: Self::CompressedItem) -> Option<Self::Item<'a>> {
        self.set.resolve(SymbolU32::try_from_usize(compressed as usize)?)
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
    fn write<'a, W: io::Write + Pos>(&self, f: &FileCompressor, slice: Self::Slice<'a>, writer: W, opt: &Options) -> Result<(Self::Size, W), Error> {
        write_string_set(&self.set, f, &slice, writer, opt)
    }
    fn read<'a, R: BetterBufRead + Pos>(f: &FileDecompressor, slice: Self::SliceMut<'a>, reader: R, size: Self::Size) -> Result<(Self, R), Error> {
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
    fn write<'a, W: io::Write + Pos>(&self, f: &FileCompressor, (prefixes, suffixes): Self::Slice<'a>, writer: W, _opt: &Options) -> Result<(Self::Size, W), Error> {
        let writer = compress_slice(f, writer, prefixes, DeltaSpec::TryLookback)?;
        let mut writer = compress_slice(f, writer, suffixes, DeltaSpec::TryLookback)?;
        
        for i in self.prefixes.iter() {
            writer.write_all(bytemuck::bytes_of(i))?;
        }
        Ok((self.prefixes.len() as u32, writer))
    }
    fn read<'a, R: BetterBufRead + Pos>(f: &FileDecompressor, (prefixes, suffixes): Self::SliceMut<'a>, reader: R, size: Self::Size) -> Result<(Self, R), Error> {
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
    fn write<'a, W: io::Write + Pos>(&self, f: &FileCompressor, slice: Self::Slice<'a>, writer: W, _opt: &Options) -> Result<(Self::Size, W), Error> {
        let writer = compress_slice(f, writer, slice, DeltaSpec::Auto)?;
        Ok(((), writer))
    }
    fn read<'a, R: BetterBufRead + Pos>(f: &FileDecompressor, slice: Self::SliceMut<'a>, reader: R, size: Self::Size) -> Result<(Self, R), Error> {
        let reader = decompress_slice(f, reader, slice)?;
        Ok((NumberSeries { _m: PhantomData }, reader))
    }
    fn get<'a>(&'a self, compressed: Self::CompressedItem) -> Option<Self::Item<'a>> {
        Some(compressed)
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
    fn write<'a, W: io::Write + Pos>(&self, f: &FileCompressor, slice: Self::Slice<'a>, mut writer: W, opt: &Options) -> Result<(Self::Size, W), Error> {
        writer.write_all(bytemuck::bytes_of(&self.offset))?;
        let writer = compress_slice(f, writer, slice, DeltaSpec::TryConsecutive(1))?;
        Ok(((), writer))
    }
    fn read<'a, R: BetterBufRead + Pos>(f: &FileDecompressor, slice: Self::SliceMut<'a>, mut reader: R, size: Self::Size) -> Result<(Self, R), Error> {
        let mut offset = 0;
        copy_to(&mut reader, bytes_of_mut(&mut offset))?;
        let reader = decompress_slice(f, reader, slice)?;
        Ok((TimeSeries { offset}, reader))
    }
    fn get<'a>(&'a self, compressed: Self::CompressedItem) -> Option<Self::Item<'a>> {
        Some(self.offset.wrapping_add(compressed as u64))
    }
}

pub fn compress_string<W: io::Write + Pos>(writer: &mut W, strings: &str, opt: &Options) -> Result<usize, Error> {
    // println!("write Brotli strings at {}", writer.pos());

    let mut params = BrotliEncoderParams::default();
    params.favor_cpu_efficiency = true;
    params.quality = opt.brotli_level as _;

    let mut input_buffer: [u8; 4096] = [0; 4096];
    let mut output_buffer: [u8; 4096] = [0; 4096];

    let mut nop_callback = |_data: &mut interface::PredictionModeContextMap<InputReferenceMut>,
                            _cmds: &mut [interface::StaticCommand],
                            _mb: interface::InputPair,
                            _m: &mut StandardAlloc| ();
                            

    let written = brotli::BrotliCompressCustomIoCustomDict(
        &mut IoReaderWrapper(&mut strings.as_bytes()),
        &mut IoWriterWrapper(writer),
        &mut input_buffer[..],
        &mut output_buffer[..],
        &params,
        StandardAlloc::default(),
        &mut nop_callback,
        opt.dict,
        io::Error::new(io::ErrorKind::UnexpectedEof, "Unexpected EOF"),

    ).unwrap();
    Ok(written)
}
fn decompress_string<R: BetterBufRead + Pos>(reader: R, len: usize) -> Result<(String, R), Error> {
    use brotli::{HeapAlloc, HuffmanCode, IoWriterWrapper};

    // println!("read Brotli strings at {}", reader.pos());

    let mut buffer: Vec<u8> = vec![];
    let mut reader = BrotliReadAdapter { inner: reader, remaining: len };

    let mut input_buffer = [0u8; 4096];
    let mut output_buffer = [0u8; 4096];
    brotli::BrotliDecompressCustomIo(
        &mut reader,
        &mut IoWriterWrapper(&mut buffer),
        &mut input_buffer[..],
        &mut output_buffer[..],
        HeapAlloc::<u8>::new(0),
        HeapAlloc::<u32>::new(0),
        HeapAlloc::<HuffmanCode>::new(HuffmanCode{ bits:2, value: 1}),
        io::Error::new(io::ErrorKind::UnexpectedEof, "Unexpected EOF")
    )?;
    let buffer = String::from_utf8(buffer)?;
    Ok((buffer, reader.inner))
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
fn decompress_slice<'a, T: Number, R: BetterBufRead + Pos>(f: &FileDecompressor, reader: R, slice: &'a mut [T]) -> Result<R, Error> {
    // println!("read [{}] at {}", type_name::<T>(), reader.pos());

    let (decompressor, reader) = f.chunk_decompressor(reader).context("chunk header")?;
    let mut page = decompressor.page_decompressor(reader, slice.len()).context("page")?;
    let progress = page.decompress(slice).context("decompress")?;
    assert!(progress.finished);
    assert_eq!(progress.n_processed, slice.len());
    
    Ok(page.into_src())
}
