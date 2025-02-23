
use std::{collections::BTreeMap, net::IpAddr, path::{Path, PathBuf}, pin::Pin};

use anyhow::Error;
use bytes::Bytes;
use clap::{arg, builder, command, Parser};
use clog_collector::{decode_batch, encode_batch, init_log, LogOptions};
use clog_core::{shema::{BatchEntry, Builder}, RequestEntry};
use futures::future::join_all;
use itertools::Itertools;
use tokio::{fs::File, io::{AsyncBufReadExt, BufReader}, spawn, sync::mpsc::{channel, Receiver}, task::JoinHandle};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    #[arg(short, long)]
    output: PathBuf,

    #[arg(short, long, default_value="10000")]
    block_size: usize,

    #[arg(short, long)]
    input: Vec<PathBuf>
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    merge(&args.input, &args.output, args.block_size).await?;
    Ok(())
}

async fn merge(input_folders: &[PathBuf], output: &PathBuf, block_size: usize) -> Result<(), Error> {
    if !output.exists() {
        tokio::fs::create_dir(output).await?;
    }
    let mut output = Writer::new(output.into(), 100_000);

    let (rxs, handles) = join(input_folders, block_size).await?;
    let mut inputs = Inputs::new(rxs).await?;

    while let Some(e) = inputs.read() {
        output.push(e).await?;
        inputs.advance().await?;
    }

    output.flush().await?;

    for h in handles {
        h.await??;
    }

    Ok(())
}

async fn join(inputs: &[PathBuf], block_size: usize) -> Result<(Vec<Receiver<Bytes>>, Vec<JoinHandle<Result<(), Error>>>), Error> {
    let mut rxs = vec![];
    let mut handles = vec![];
    for path in inputs {
        if path.is_dir() {
            let (rx, handle) = read_buffers(path).await?;
            rxs.push(rx);
            handles.push(handle);
        } else {
            let (rx, handle) = read_log(path, block_size).await?;
            rxs.push(rx);
            handles.push(handle);
        }
    }

    Ok((rxs, handles))
}

async fn read_buffers(path: &Path) -> Result<(Receiver<Bytes>, JoinHandle<Result<(), Error>>), Error> {
    let mut dir = tokio::fs::read_dir(path).await?;

    let mut entries: BTreeMap<u64, PathBuf> = BTreeMap::new();

    let (tx, rx) = channel(4);
    while let Some(entry) = dir.next_entry().await? {
        let path = entry.path();
        if path.extension().map(|e| e == "clog").unwrap_or(false) {
            if let Some(n) = path.file_stem().and_then(|s| s.to_str()).and_then(|s| s.strip_prefix("block-")).and_then(|s| s.parse::<u64>().ok()) {
                println!("  block {n}");
                entries.insert(n, path);
            }
        }
    }
    let handle = spawn(async move {
        for (n, path) in entries {
            let data = tokio::fs::read(path).await?;
            tx.send(data.into()).await?;
        }
        Result::<(), Error>::Ok(())
    });
    Ok((rx, handle))
}

async fn read_log(path: &Path, block_size: usize) -> Result<(Receiver<Bytes>, JoinHandle<Result<(), Error>>), Error> {
    let file = File::open(path).await?;
    let mut reader = BufReader::new(file);

    let mut line = String::new();

    let mut builder = Builder::with_capacity(block_size);
    let mut start = 0;

    let (tx, rx) = channel(4);
    let handle = spawn(async move {
        loop {
            let n = reader.read_line(&mut line).await?;
            if n == 0 {
                break;
            }
            if let Ok(out) = serde_json::from_str::<RequestEntry>(&line) {
                builder.add(BatchEntry::from(&out));
                if builder.len() >= block_size {
                    let bytes = encode_batch(start, &builder, 11);
                    tx.send(bytes).await?;

                    start += builder.len() as u64;
                    builder = Builder::with_capacity(block_size);
                }
            }
            line.clear();
        }
        if builder.len() > 0 {
            let bytes = encode_batch(start, &builder, 11);
            tx.send(bytes).await?;
        }
        Result::<(), Error>::Ok(())
    });

    Ok((rx, handle))
}

struct Input {
    t: u64,
    builder: Builder,
    rx: Receiver<Bytes>,
    pos: usize,
}

struct Inputs {
    inputs: Vec<Input>,
    next_idx: usize,
}
impl Inputs {
    pub async fn new(rxs: Vec<Receiver<Bytes>>) -> Result<Self, Error> {
        let mut inputs = Vec::with_capacity(rxs.len());

        println!("{} channels", rxs.len());
        for (j, mut rx) in rxs.into_iter().enumerate() {
            if let Some(batch) = rx.recv().await {
                let (_, builder) = decode_batch(&batch)?;
                println!("{j} batch with {} items", builder.len());
                if let Some(e) = builder.get(0) {
                    inputs.push(Input { t: e.time, builder, rx, pos: 0 });
                }
            } else {
                println!("{j} no input");
            }
        }
        let mut i = Inputs { inputs, next_idx: 0 };
        i.find_next();
        Ok(i)
    }
    
    pub fn read(&self) -> Option<BatchEntry> {
        let i = self.inputs.get(self.next_idx)?;
        i.builder.get(i.pos)
    }
    fn find_next(&mut self) -> Option<u64> {
        let (idx, i) = self.inputs.iter().enumerate().min_by_key(|(n, i)| i.t)?;
        self.next_idx = idx;
        Some(i.t)
    }

    pub async fn advance(&mut self) -> Result<Option<u64>, Error> {
        while self.inputs.len() > 0 {
            let Some(i) = self.inputs.get_mut(self.next_idx) else { return Ok(None) };
            i.pos += 1;
            match i.builder.get(i.pos) {
                Some(e) => {
                    i.t = e.time;
                    return Ok(self.find_next());
                }
                None => {
                    if let Some(batch) = i.rx.recv().await {
                        let (_, builder) = decode_batch(&batch)?;
                        println!("new batch with {} items", builder.len());
                        if let Some(e) = builder.get(0) {
                            let t = e.time;
                            i.builder = builder;
                            i.pos = 0;
                            i.t = t;
                            return Ok(Some(t));
                        }
                    }
                }
            }
            println!("input {} exhausted", self.next_idx);
            println!("next t={}", self.inputs.iter().map(|i| i.t).format(", "));
            self.inputs.remove(self.next_idx);
            self.find_next();
        }
        Ok(None)
    }
}

struct Writer {
    folder: PathBuf,
    current: Builder,
    current_start: u64,
    block_limit: usize,
}
impl Writer {
    pub fn new(folder: PathBuf, block_limit: usize) -> Self {
        Writer {
            folder,
            current: Builder::with_capacity(block_limit),
            current_start: 0,
            block_limit
        }
    }
    async fn push<'a>(&mut self, entry: BatchEntry<'a>) -> Result<(), Error> {
        self.current.add(entry);

        if self.current.len() >= self.block_limit {
            self.flush().await?;
        }
        Ok(())
    }
    async fn flush(&mut self) -> Result<(), Error> {
        if self.current.len() > 0 {
            let data = encode_batch(self.current_start, &self.current, 11);
            let path = self.folder.join(format!("block-{}.clog", self.current_start));

            tokio::fs::write(path, &data).await?;
            self.current_start += self.current.len() as u64;
            self.current = Builder::with_capacity(self.block_limit);
        }
        Ok(())
    }
}
