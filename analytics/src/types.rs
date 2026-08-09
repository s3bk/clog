use serde::{Deserialize, Serialize};
use std::net::IpAddr;

pub type ReqItem<'a> = clog_core::shema::BatchEntry<'a>;
pub use clog_core::Protocol;
