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
