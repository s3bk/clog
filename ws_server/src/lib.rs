use axum::{body::Bytes, extract::ws::{Message, WebSocket}};
use clog_core::PacketType;
use clog_ws_api::{ClientMessage, ServerMessage};

use clog_collector::{ClientHandle, LogCollector};
use tokio::{select, sync::broadcast};

struct ClientState {
    log: LogCollector,
    handle: Option<ClientHandle>,
    ws: WebSocket,
}
impl ClientState {
    async fn handle_packet(&mut self, msg: Message) {
        match msg {
            Message::Binary(data) => {
                let Ok(msg) = postcard::from_bytes::<ClientMessage>(&data) else { return };

                match msg {
                    ClientMessage::FetchRange { start, end } => {
                        match self.handle {
                            Some(ref mut h) => {
                                h.get_range(start, end).await;
                            }
                            None => {
                                self.send_msg(ServerMessage::NotAttached).await;
                            }

                        }
                    },
                    ClientMessage::Subscribe => {
                        self.handle = self.log.attach().await.ok();
                    }
                    ClientMessage::SubScribeWithBacklog { backlog } => {
                        self.handle = self.log.attach_with_backlog(backlog).await.ok();
                    }
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
                self.ws.send(Message::Binary(bytes)).await;
            }
            Err(_) => {
                self.send_msg(ServerMessage::Detached).await;
                self.handle = None;
            }
        }
    }
}

pub async fn handle_ws(ws: WebSocket, log: LogCollector) {
    let mut state = ClientState { handle: None, ws, log };

    loop {
        if let Some(ref mut handle) = state.handle {
            select! {
                Some(Ok(msg)) = state.ws.recv() => {
                    state.handle_packet(msg).await;
                }
                Some(bytes) = handle.batch_rx.recv() => {
                    state.ws.send(Message::Binary(bytes)).await;
                }
                r = handle.row_rx.recv() => {
                    state.handle_row(r).await;
                }
                else => {
                    break
                }
            }   
        } else {
            match state.ws.recv().await {
                Some(Ok(msg)) => state.handle_packet(msg).await,
                _ => break
            }
        }
    }
}

