use std::time::Duration;

use axum::{body::Bytes, extract::ws::{Message, WebSocket}};
use clog_core::PacketType;
use clog_ws_api::{ClientMessage, ServerMessage};

use clog_collector::{ClientHandle, LogCollector};
use tokio::{select, sync::broadcast, time::{interval, sleep, Interval}};

struct ClientState {
    log: LogCollector,
    handle: Option<ClientHandle>,
    ws: WebSocket,
    ping_timer: Interval,
    last_pong: u32,
    last_ping: u32,
    closed: bool,
}
impl ClientState {
    async fn handle_packet(&mut self, msg: Message) {
        match msg {
            Message::Binary(data) => {
                let Ok(msg) = postcard::from_bytes::<ClientMessage>(&data) else { return };

                match msg {
                    ClientMessage::FetchRange { start, end } => {
                        println!("fetch {start}..{end}");
                        match self.handle {
                            Some(ref mut h) => {
                                h.get_range(start, end).await;
                            }
                            None => {
                                self.send_msg(ServerMessage::NotAttached).await;
                            }

                        }
                    },
                    ClientMessage::SubScribeWithBacklog { backlog } => {
                        self.handle = self.log.attach_with_backlog(backlog).await.ok();
                    }
                }
            }
            Message::Ping(data) => {
                self.ws.send(Message::Pong(data));
            }
            Message::Pong(data) => {
                if let Ok(bytes) = <[u8; 4]>::try_from(&*data) {
                    let n = u32::from_be_bytes(bytes);
                    self.last_pong = n;
                }
            }
            _ => {}
        }
    }
    async fn send_msg(&mut self, msg: ServerMessage) {
        let data = msg.encode();
        self.ws.send(Message::Binary(data.into())).await;
    }
    async fn handle_row(&mut self, r: Result<Bytes, broadcast::error::RecvError>) {
        match r {
            Ok(bytes) => {
                self.ws.send(Message::Binary(bytes.into())).await;
            }
            Err(_) => {
                self.send_msg(ServerMessage::Detached).await;
                self.handle = None;
            }
        }
    }
    async fn tick(&mut self) {
        if self.last_pong < self.last_ping {
            self.ws.send(Message::Close(None)).await;
            self.closed = true;
        }
        self.last_ping += 1;
        self.ws.send(Message::Ping(self.last_ping.to_be_bytes().as_slice().into())).await;
    }
}

pub async fn handle_ws(ws: WebSocket, log: LogCollector) {
    let ping_timer = interval(Duration::from_secs(10));
    let mut state = ClientState { handle: None, ws, log, ping_timer, last_pong: 0, last_ping: 0, closed: false };

    while !state.closed {
        if let Some(ref mut handle) = state.handle {
            select! {
                Some(Ok(msg)) = state.ws.recv() => {
                    state.handle_packet(msg).await;
                }
                Some(bytes) = handle.batch_rx.recv() => {
                    state.ws.send(Message::Binary(bytes.into())).await;
                }
                r = handle.row_rx.recv() => {
                    state.handle_row(r).await;
                }
                _ = state.ping_timer.tick() => {
                    state.tick().await;
                }
                else => {
                    break
                }
            }   
        } else {
            select! {
                Some(Ok(msg)) = state.ws.recv() => {
                    state.handle_packet(msg).await;
                }
                _ = state.ping_timer.tick() => {
                    state.tick().await;
                }
                else => {
                    break
                }
            }
        }
    }
}

