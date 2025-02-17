use std::{collections::{BTreeMap, VecDeque}, io::Cursor, mem::replace, sync::Arc};

use anyhow::Error;
use bytemuck::bytes_of;
use bytes::{Bytes, BytesMut};
use tokio::{select, sync::{broadcast, mpsc::{channel, Receiver, Sender}, oneshot}, task::spawn_blocking};

use crate::{shema::{BatchEntry, Builder}, BatchHeader, PacketType, RequestEntry};

enum ClientMsg {
    Attach { tx: oneshot::Sender<broadcast::Receiver<Bytes>> },
    AttachWithBacklog { batch_tx: Sender<Bytes>, backlog: usize, tx: oneshot::Sender<broadcast::Receiver<Bytes>> },
    GetRange { start: u64, end: u64, tx: Sender<Bytes> }
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
    pub async fn attach(&self) -> Result<ClientHandle, Error> {
        let (oneshot_tx, oneshot_rx) = oneshot::channel();
        let (batch_tx, batch_rx) = channel(128);
        
        self.tx.send(ClientMsg::Attach { tx: oneshot_tx }).await?;
        let row_rx = oneshot_rx.await?;

        Ok(ClientHandle { row_rx, batch_rx, batch_tx, tx: self.tx.clone() })
    }
    pub async fn attach_with_backlog(&self, backlog: usize) -> Result<ClientHandle, Error> {
        let (oneshot_tx, oneshot_rx) = oneshot::channel();
        let (batch_tx, batch_rx) = channel(128);
        
        self.tx.send(ClientMsg::AttachWithBacklog { batch_tx: batch_tx.clone(), backlog, tx: oneshot_tx }).await?;
        let row_rx = oneshot_rx.await?;

        Ok(ClientHandle { row_rx, batch_rx, batch_tx, tx: self.tx.clone() })
    }
}
impl ClientHandle {
    pub async fn get_range(&self, start: u64, end: u64) -> Result<(), Error> {
        self.tx.send(ClientMsg::GetRange { start, end, tx: self.batch_tx.clone() }).await?;
        Ok(())
    }
}

pub fn init_log() -> (LogCollector, Sender<RequestEntry>) {
    let (client_tx, mut client_rx) = channel(128);
    let (past_tx, past_rx) = channel(128);
    let (row_tx, row_rx) = broadcast::channel(4096);
    let (event_tx, mut event_rx) = channel::<RequestEntry>(128);

    let mut backend = CollectorBackend {
        past_tx,
        block_limit: 10_000,
        current: Builder::default(),
        current_start: 0,
        tx: row_tx
    };

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
        let mut past = PastManager {
            past_buffers: Default::default(),
            past_rx
        };
        past.run().await;
    });

    (LogCollector { tx: client_tx }, event_tx)
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
            self.send_current();
        }
    }
    fn send_current(&mut self) {
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
        });
    }
    async fn send_sync(&self, tx: &Sender<Bytes>) {
        let mut sync_buf = BytesMut::with_capacity(1+8);
        PacketType::Sync.write_to(&mut sync_buf);
        let sync_buf = postcard::to_extend(&BatchHeader { start: self.current_start + self.current.len() as u64 }, sync_buf).unwrap();
        tx.send(sync_buf.into()).await;
    }
    fn get_current(&self, tx: Sender<Bytes>) -> u64 {
        let start = self.current_start;
        let current = self.current.clone();
        spawn_blocking(move || {
            let data = encode_batch(start, &current, 5);
            let _ = tx.blocking_send(data.into());
        });

        start
    }
    pub fn follow(&self) -> broadcast::Receiver<Bytes> {
        self.tx.subscribe()
    }
    pub async fn follow_with_backlog(&self, backlog: u64, batch_tx: Sender<Bytes>) -> broadcast::Receiver<Bytes> {
        self.send_sync(&batch_tx).await;
        let current = self.get_current(batch_tx.clone());
        let row_rx = self.tx.subscribe();
        self.past_tx.send(PastCommand::Get { start: current.saturating_sub(backlog), end: current, tx: batch_tx }).await.unwrap();
        row_rx
    }
    pub async fn get_range(&self, start: u64, end: u64, batch_tx: Sender<Bytes>) {
        self.past_tx.send(PastCommand::Get { start, end, tx: batch_tx }).await.unwrap();
    }
    pub async fn handle_msg(&mut self, msg: ClientMsg) {
        match msg {
            ClientMsg::Attach { tx } => {
                let rx = self.follow();
                tx.send(rx);
            }
            ClientMsg::AttachWithBacklog { batch_tx, backlog, tx } => {
                let rx = self.follow_with_backlog(backlog as _, batch_tx).await;
                tx.send(rx);
            }
            ClientMsg::GetRange { start, end, tx } => {
                self.get_range(start, end, tx).await;
            }
        }
    }
    async fn flush(&mut self) {
        let (tx, rx) = oneshot::channel();
        self.send_current();
        self.past_tx.send(PastCommand::Flush { tx }).await;
        rx.await;
    }
}

fn encode_batch(start: u64, builder: &Builder, brotli_level: u8) -> Bytes {
    let mut buffer = BytesMut::with_capacity(builder.len() * 10);
    PacketType::Batch.write_to(&mut buffer);
    let buffer = postcard::to_extend(&BatchHeader {
        start
    }, buffer).unwrap();

    let data = builder.write_to(buffer, &crate::Options { brotli_level, dict: &[] });
    data.into()
}

enum PastCommand {
    AddBuffer { start: u64, data: Bytes },
    Get { start: u64, end: u64, tx: Sender<Bytes> },
    Flush { tx: oneshot::Sender<()> }
}

struct PastManager {
    past_rx: Receiver<PastCommand>,
    past_buffers: BTreeMap<u64, Bytes>
}
impl PastManager {
    async fn run(&mut self) {
        while let Some(cmd) = self.past_rx.recv().await {
            match cmd {
                PastCommand::AddBuffer { start, data } => {
                    self.past_buffers.insert(start, data);
                }
                PastCommand::Get { start, end, tx } => {
                    for (_, data) in self.past_buffers.range(start..end) {
                        let _ = tx.send(data.clone()).await;
                    }
                }
                PastCommand::Flush { tx } => {
                    let _ = tx.send(());
                }
            }
        }
    }
}
