use bytes::{Bytes, BytesMut};
use clog_core::{PacketType, SyncHeader};
use serde::{Deserialize, Serialize};


#[derive(Serialize, Deserialize)]
pub enum ClientMessage {
    Subscribe,
    SubScribeWithBacklog { backlog: usize },
    FetchRange { start: u64, end: u64 },
}

#[derive(Serialize, Deserialize)]
pub enum ServerMessage {
    NotAttached,
    Detached,
    Error { msg: String }
}
impl ServerMessage {
    pub fn encode(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(32);
        PacketType::ServerMsg.write_to(&mut bytes);
        postcard::to_extend(self, bytes).unwrap().into()
    }
}
