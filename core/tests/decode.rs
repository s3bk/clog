use clog_core::{
    shema::{Builder, ShemaImplBuilder, Shema, BatchEntry},
    Options, RequestEntry,
    util::IoWritePos,
    types::compress_string,
    BuildHasher,
};
use serde_json::Value;
use std::{
    io::{BufRead, BufReader},
    fs::File
};

#[test]
fn old() {
    let data = std::fs::read("../logs/block-10001.clog").unwrap();
    Builder::from_slice(&data).unwrap();
}

#[test]
fn new() {
    let data = std::fs::read("../logs/block-10001.clog").unwrap();
    ShemaImplBuilder::from_slice(&data).unwrap();
}

fn read_log() -> impl Iterator<Item=RequestEntry> {
    let file = File::open("../logs/user.log").unwrap();
    let mut reader = BufReader::new(file);

    let mut line = String::new();

    std::iter::from_fn(move || {
        let n = reader.read_line(&mut line).ok()?;
        if n == 0 {
            return None;
        }
        let val = serde_json::from_str::<Value>(&line).ok()?;
        line.clear();

        let out = serde_json::from_value::<RequestEntry>(val);
        match out {
            Ok(v) => Some(Some(v)),
            Err(e) => {
                //dbg!(e);
                Some(None)
            }
        }
    }).flatten()
}

#[test]
fn test_log() {
    let mut builder = Builder::default();
    let entries: Vec<RequestEntry> = read_log().collect();
    for entry in entries.iter() {
        builder.add(entry.into());
    }
    println!("parsing complete. {} entries", builder.len());

    for q in 1 .. 2 {
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
    let b2 = Builder::from_slice(&data).unwrap();

    for item in b2.iter() {
        println!("{item:?}");
    }
}

#[test]
fn test_compression() {
    use std::collections::HashSet;

    fn test_dict(uris: &str, opt: &Options) -> usize {
        let mut out = IoWritePos { writer: vec![], pos: 0 };
        compress_string(&mut out, uris, &opt).unwrap();
        out.writer.len()
    }

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
