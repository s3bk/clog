use std::{collections::{BTreeMap, BTreeSet, VecDeque}, io::Cursor, mem::replace, path::PathBuf, sync::Arc};
use anyhow::{bail, Error};
use bytes::{Bytes, BytesMut};
use tokio::{select, sync::{broadcast, mpsc::{channel, Receiver, Sender}, oneshot}, task::spawn_blocking};

use clog_core::{shema::{BatchEntry, Builder}, BatchHeader, PacketType, RequestEntry, SyncHeader};

enum ClientMsg {
    AttachWithBacklog { batch_tx: Sender<Bytes>, backlog: usize, tx: oneshot::Sender<broadcast::Receiver<Bytes>> },
    GetRange { start: u64, end: u64, tx: Sender<Bytes> },
    Flush { tx: oneshot::Sender<Result<(), ()>> },
}

#[derive(Clone)]
pub struct LogCollector {
    tx: Sender<ClientMsg>,
}

pub struct ClientHandle {
    tx: Sender<ClientMsg>,
    pub row_rx: broadcast::Receiver<Bytes>,
    batch_tx: Sender<Bytes>,
    pub batch_rx: Receiver<Bytes>,
}

impl LogCollector {
    pub async fn attach_with_backlog(&self, backlog: usize) -> Result<ClientHandle, Error> {
        let (oneshot_tx, oneshot_rx) = oneshot::channel();
        let (batch_tx, batch_rx) = channel(128);
        
        self.tx.send(ClientMsg::AttachWithBacklog { batch_tx: batch_tx.clone(), backlog, tx: oneshot_tx }).await?;
        let row_rx = oneshot_rx.await?;

        Ok(ClientHandle { row_rx, batch_rx, batch_tx, tx: self.tx.clone() })
    }
    pub async fn flush(&self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(ClientMsg::Flush { tx }).await?;
        rx.await?.map_err(|_| anyhow::anyhow!("flush not successful"))?;
        Ok(())
    }
}
impl ClientHandle {
    pub async fn get_range(&self, start: u64, end: u64) -> Result<(), Error> {
        self.tx.send(ClientMsg::GetRange { start, end, tx: self.batch_tx.clone() }).await?;
        Ok(())
    }
}

pub struct LogOptions {
    pub data_dir: Option<PathBuf>,
    pub read_old: bool,
}

pub async fn init_log(options: LogOptions) -> Result<(LogCollector, Sender<RequestEntry>), Error> {
    let (client_tx, mut client_rx) = channel(128);
    let (past_tx, past_rx) = channel(128);
    let (row_tx, row_rx) = broadcast::channel(4096);
    let (event_tx, mut event_rx) = channel::<RequestEntry>(128);

    let mut past = PastManager {
        past_buffers: Default::default(),
        past_rx,
        dir: options.data_dir,
    };
    
    let mut backend = CollectorBackend {
        past_tx,
        block_limit: 10_000,
        current: Builder::default(),
        current_start: 0,
        tx: row_tx
    };

    if options.read_old {
        past.read().await?;
        if let Some((start, data)) = past.take_last().await? {
            let (start2, builder) = decode_batch(&data)?;
            if start != start2 {
                bail!("header mismatch {start} != {start2}");
            }
            backend.current = builder;
            backend.current_start = start;
            println!("resume log at {start}");
        }
    }


    tokio::spawn(async move {
        loop {
            select! {
                Some(e) = event_rx.recv() => {
                    backend.push((&e).into());
                }
                Some(msg) = client_rx.recv() => {
                    backend.handle_msg(msg).await;
                }
                else => break
            }
        }
    });

    tokio::spawn(async move {
        past.run().await;
    });

    Ok((LogCollector { tx: client_tx }, event_tx))
}



struct CollectorBackend {
    past_tx: Sender<PastCommand>,
    current: Builder,
    current_start: u64,
    tx: broadcast::Sender<Bytes>,
    block_limit: usize
}
impl CollectorBackend {
    fn push<'a>(&mut self, entry: BatchEntry<'a>) {
        if self.tx.receiver_count() > 0 {
            let mut buf = BytesMut::with_capacity(100);
            PacketType::Row.write_to(&mut buf);
            let buf = postcard::to_extend(&entry, buf).unwrap();
            let _ = self.tx.send(buf.into());
        }
        
        self.current.add(entry);
        if self.current.len() >= self.block_limit {
            self.send_current(None);
        }
    }
    fn send_current(&mut self, flush_tx: Option<oneshot::Sender<()>>) {
        if self.current.len() == 0 {
            return;
        }
        let builder = replace(&mut self.current, Builder::default());
        let builder_start = self.current_start;
        self.current_start += builder.len() as u64;
        let tx = self.past_tx.clone();

        spawn_blocking(move || {
            let data = encode_batch(builder_start, &builder, 11);
            let _ = tx.blocking_send(PastCommand::AddBuffer { start: builder_start, data });
            if let Some(flush_tx) = flush_tx {
                let _ = tx.blocking_send(PastCommand::Flush { tx: flush_tx });
            }
        });
    }
    async fn send_sync(&self, tx: &Sender<Bytes>, first_backlog: u64) {
        let info = SyncHeader {
            block_size: self.block_limit,
            first_backlog,
            first_block: 0,
            start: self.current_start + self.current.len() as u64
        };
        let mut sync_buf = BytesMut::with_capacity(32);
        PacketType::Sync.write_to(&mut sync_buf);
        let sync_buf = postcard::to_extend(&info, sync_buf).unwrap();
        tx.send(sync_buf.into()).await;
    }
    fn get_current(&self, tx: Sender<Bytes>) -> u64 {
        let start = self.current_start;
        if self.current.len() > 0 {
            let current = self.current.clone();
            spawn_blocking(move || {
                let data = encode_batch(start, &current, 5);
                let _ = tx.blocking_send(data.into());
            });
        }
        start
    }
    pub async fn follow_with_backlog(&self, backlog: u64, batch_tx: Sender<Bytes>) -> broadcast::Receiver<Bytes> {
        let first_backlog = self.current_start.saturating_sub(backlog);
        self.send_sync(&batch_tx, first_backlog).await;

        let current = self.get_current(batch_tx.clone());
        let row_rx = self.tx.subscribe();
        self.past_tx.send(PastCommand::Get { start: first_backlog , end: current, tx: batch_tx }).await.unwrap();
        row_rx
    }
    pub async fn get_range(&self, start: u64, end: u64, batch_tx: Sender<Bytes>) {
        self.past_tx.send(PastCommand::Get { start, end, tx: batch_tx }).await.unwrap();
    }
    pub async fn handle_msg(&mut self, msg: ClientMsg) {
        match msg {
            ClientMsg::AttachWithBacklog { batch_tx, backlog, tx } => {
                let rx = self.follow_with_backlog(backlog as _, batch_tx).await;
                let _ = tx.send(rx);
            }
            ClientMsg::GetRange { start, end, tx } => {
                self.get_range(start, end, tx).await;
            }
            ClientMsg::Flush { tx } => {
                let r = self.flush().await.map_err(|_| ());
                let _ = tx.send(r);
            }
        }
    }
    async fn flush(&mut self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.send_current(Some(tx));

        rx.await?;
        Ok(())
    }
}

fn encode_batch(start: u64, builder: &Builder, brotli_level: u8) -> Bytes {
    let mut buffer = BytesMut::with_capacity(builder.len() * 10);
    PacketType::Batch.write_to(&mut buffer);
    let buffer = postcard::to_extend(&BatchHeader {
        start
    }, buffer).unwrap();

    let data = builder.write_to(buffer, &clog_core::Options { brotli_level, dict: &[] });
    data.into()
}
fn decode_batch(data: &[u8]) -> Result<(u64, Builder), Error> {
    let (&ptype, data) = data.split_first().ok_or(anyhow::anyhow!("no data"))?;

    if ptype != PacketType::Batch as u8 {
        bail!("invalid header");
    }

    let (header, data) = postcard::take_from_bytes::<BatchHeader>(data)?;
    let builder = Builder::from_slice(data)?;
    Ok((header.start, builder))
}

enum PastCommand {
    AddBuffer { start: u64, data: Bytes },
    Get { start: u64, end: u64, tx: Sender<Bytes> },
    Flush { tx: oneshot::Sender<()> }
}

struct PastManager {
    past_rx: Receiver<PastCommand>,
    past_buffers: BTreeMap<u64, Option<Bytes>>,
    dir: Option<PathBuf>,
}
impl PastManager {
    async fn run(&mut self) {
        while let Some(cmd) = self.past_rx.recv().await {
            match cmd {
                PastCommand::AddBuffer { start, data } => {
                    println!("add buffer at {}", start);
                    if let Some(ref root) = self.dir {
                        let path = root.join(format!("block-{start}.clog"));
                        tokio::fs::write(path, &data).await;
                    }
                    self.past_buffers.insert(start, Some(data));
                }
                PastCommand::Get { start, end, tx } => {
                    println!("GET {start}..{end}");
                    for (&pos, data) in self.past_buffers.range_mut(..end).rev() {
                        if data.is_none() {
                            if let Some(ref dir) = self.dir {
                                let path = dir.join(format!("block-{start}.clog"));
                                println!("reading {path:?}");
                                if let Ok(new) = tokio::fs::read(path).await {
                                    let bytes = Bytes::from(new);
                                    *data = Some(bytes.clone());
                                }
                            }
                        };
                        if let Some(data) = data {
                            let _ = tx.send(data.clone()).await;
                        }
                        if pos < start {
                            break;
                        }
                    }
                }
                PastCommand::Flush { tx } => {
                    let _ = tx.send(());
                }
            }
        }
    }

    async fn take_last(&mut self) -> Result<Option<(u64, Bytes)>, Error> {
        if let Some((start, data)) = self.past_buffers.pop_last() {
            if let Some(data) = data {
                return Ok(Some((start, data)));
            }
            if let Some(ref dir) = self.dir {
                let path = dir.join(format!("block-{start}.clog"));
                println!("reading {path:?}");
                if let Ok(new) = tokio::fs::read(path).await {
                    let bytes = Bytes::from(new);
                    return Ok(Some((start, bytes)));
                }
            }
        }
        Ok(None)
    }

    async fn read(&mut self) -> Result<(), Error> {
        let Some(ref path) = self.dir else { return Ok(()) };
        let mut dir = tokio::fs::read_dir(path).await?;

        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();
            if path.extension().map(|e| e == "clog").unwrap_or(false) {
                if let Some(n) = path.file_stem().and_then(|s| s.to_str()).and_then(|s| s.strip_prefix("block-")).and_then(|s| s.parse::<u64>().ok()) {
                    println!("  block {n}");
                    self.past_buffers.insert(n, None);
                }
            }
        }
        Ok(())
    }
}
