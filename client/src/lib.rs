use std::{collections::{BTreeMap, HashMap, VecDeque}, ops::Range, sync::Arc};

use js_sys::{Function, JsString, Uint8Array};
use serde::Serialize;
use wasm_bindgen::{prelude::wasm_bindgen, JsValue};
use web_sys::{BinaryType, Event, MessageEvent, WebSocket};
use ouroboros::self_referencing;
use clog_core::{filter::Filter, shema, BatchHeader, PacketType};
use clog_ws_api::{ClientMessage, ServerMessage};

use crate::shema::{BatchEntry, Builder};

macro_rules! debug {
    ($($t:tt)*) => ({
        web_sys::console::debug_1(&JsValue::from_str(&format!($($t)*)))
    });
}

#[wasm_bindgen]
pub struct Client {
    websocket: WebSocket,

    entries: BTreeMap<u64, Builder>,
    current: Builder,
    current_start: u64,
}

#[wasm_bindgen]
pub struct PacketRange {
    pub start: u64,
    pub end: u64
}

#[wasm_bindgen]
impl Client {
    #[wasm_bindgen(constructor)]
    pub fn new(websocket: WebSocket) -> Self {
        websocket.set_binary_type(BinaryType::Arraybuffer);
    
        Client {
            entries: Default::default(),
            current: Builder::default(),
            current_start: 0,
            websocket,
        }
    }
    fn send(&self, msg: ClientMessage) {
        let str = serde_json::to_string(&msg).unwrap();
        self.websocket.send_with_str(&str);
    }
    pub fn on_open(&mut self, e: Event) {
        self.send(ClientMessage::SubScribeWithBacklog { backlog: 1000 });
    }
    pub fn on_message(&mut self, event: MessageEvent) -> Option<PacketRange> {
        let data = event.data();
        if let Some(json) = data.as_string() {
            let msg = serde_json::from_str::<ServerMessage>(&json).unwrap();
            match msg {
                ServerMessage::Detached | ServerMessage::NotAttached => {
                    self.send(ClientMessage::Subscribe);
                }
                _ => {}
            }
            None
        } else {
            let data = Uint8Array::new(&data);
            let data = data.to_vec();
            self.handle_packet(&data).map(|r| PacketRange { start: r.start, end: r.end })
        }
    }
    fn get_entry(&self, n: u64) -> Option<BatchEntry> {
        if n >= self.current_start {
            if let Some(val) = self.current.get((n - self.current_start) as usize) {
                return Some(val);
            }
        }
        if let Some((&start, chunk)) = self.entries.range(..=n).rev().next() {
            if start <= n && start + chunk.len() as u64 > n {
                let val = chunk.get((n - start) as usize).unwrap();
                return Some(val)
            }
        }
        None
    }
    fn get_range(&self, range: Range<u64>) -> impl Iterator<Item=(u64, BatchEntry)> + DoubleEndedIterator {
        let Range { start, end } = range;
        self.entries.range(..range.start).rev().next().into_iter().chain(self.entries.range(range)).chain(std::iter::once((&self.current_start, &self.current)))
            .flat_map(move |(&n, chunk)| {
                let start = start.saturating_sub(n).min(chunk.len() as u64) as usize;
                let end = end.saturating_sub(n).min(chunk.len() as u64) as usize;
                chunk.range(start..end).enumerate().map(move |(i, e)| ((i + start) as u64 + n, e))
            })
    }
    pub fn get(&self, n: u64) -> JsValue {
        match self.get_entry(n) {
            None => JsValue::null(),
            Some(e) => wrap(e)
        }
    }
    pub fn end(&self) -> u64 {
        (self.current_start + self.current.len() as u64).max(self.entries.iter().rev().next().map(|(k, v)| k + v.len() as u64).unwrap_or(0))
    }
    fn handle_packet(&mut self, data: &[u8]) -> Option<Range<u64>> {
        let (&typ_byte, rest) = data.split_first()?;
        let typ = PacketType::parse(typ_byte)?;

        match typ {
            PacketType::Batch => {
                let (header, rest) = postcard::take_from_bytes::<BatchHeader>(rest).ok()?;
                let builder = Builder::from_slice(rest).ok()?;
                let range = header.start .. header.start + builder.len() as u64;
                self.entries.insert(header.start, builder);
                
                debug!("BATCH {range:?}");
                Some(range)
            }
            PacketType::Row => {
                let row = postcard::from_bytes::<BatchEntry>(rest).ok()?;
                
                let start = self.current_start + self.current.len() as u64;
                self.current.add(row);

                Some(start .. start+1)
            }
            PacketType::Sync => {
                let (header, _) = postcard::take_from_bytes::<BatchHeader>(rest).ok()?;
                self.current_start = header.start;
                debug!("SYNC to {}", header.start);
                None
            }
        }
    }
}

#[wasm_bindgen]
pub struct ScrollView {
    // BatchEntry -> T
    produce: Function,

    // T[]
    current: VecDeque<JsValue>,
    current_start: u64,

    start: u64,
    len: usize,
}

#[wasm_bindgen]
impl ScrollView {
    #[wasm_bindgen(constructor)]
    pub fn new(produce: Function, len: usize) -> Self {
        ScrollView {
            produce,
            current: VecDeque::with_capacity(len),
            current_start: 0,
            start: 0,
            len
        }
    }
    pub fn scroll_by(&mut self, client: &Client, by: i32) {
        if by > 0 {
            let max = client.end() - self.len as u64;
            self.start = (self.start + by as u64).min(max);
        } else {
            self.start = self.start.saturating_sub((-by) as u64);
        }
    }
    pub fn scroll_to(&mut self, pos: u64) {
        self.start = pos;
    }
    pub fn pos(&self) -> u64 {
        self.start
    }
    fn produce(&self, n: u64, e: BatchEntry<'_>) -> Result<JsValue, JsValue> {
        self.produce.call2(&JsValue::null(), &serde_wasm_bindgen::to_value(&n).unwrap_or_default(), &wrap(e))
    }
    pub fn render(&mut self, client: &Client) -> Result<Vec<JsValue>, JsValue> {
        if self.start > self.current_start {
            // trim some from the front and add to the end
            let offset = (self.start - self.current_start) as usize;
            if offset >= self.current.len() {
                self.current.clear();
            } else {
                self.current.drain(..offset);
            }
            let i0 = self.current.len();
            for i in i0 .. self.len {
                let n = self.start + i as u64;
                if let Some(e) = client.get_entry(n) {
                    let val = self.produce(n, e)?;
                    self.current.push_back(val);
                }
            }
        } else {
            // trim from the end and add to the front
            let offset = (self.current_start - self.start) as usize;
            let end = self.current.len().saturating_sub(offset);
            self.current.drain(end..);
            let i1 = self.len - self.current.len();
            for i in (0 .. i1).rev() {
                let n = self.start + i as u64;
                if let Some(e) = client.get_entry(n) {
                    let val = self.produce(n, e)?;
                    self.current.push_front(val);
                }
            }
        }
        self.current_start = self.start;

        Ok(self.current.iter().cloned().collect())
    }
}

#[wasm_bindgen]
pub struct FilterView {
    // (n: bigint, e: BatchEntry) -> JsValue
    produce: Function,
    len: usize,
    filter: Option<Filter>,

    cache: HashMap<u64, JsValue>,
    start: u64,
}
#[wasm_bindgen]
impl FilterView {
    #[wasm_bindgen(constructor)]
    pub fn new(produce: Function, len: usize) -> Self {
        FilterView {
            produce,
            len,
            filter: None,
            cache: Default::default(),
            start: 0,
        }
    }

    pub fn scroll_to(&mut self, pos: u64) {
        self.start = pos;
    }
    pub fn scroll_by(&mut self, client: &Client, by: isize) {
        if by > 0 {
            if let Some((pos, _)) = client.get_range(self.start .. u64::MAX).filter(|(_, e)| matches(&self.filter, e)).take(by as usize + 1).last() {
                self.start = pos;
            }
        } else if by < 0 {
            let pos = client.get_range(0 .. self.start).rev().filter(|(_, e)| matches(&self.filter, e)).take((-by) as usize + 1).last().map(|(pos, _)| pos).unwrap_or(0);
            self.start = pos;
        }
    }

    pub fn set_filter(&mut self, filter: JsValue) -> Result<(), JsValue> {
        self.filter = serde_wasm_bindgen::from_value(filter)?;
        Ok(())
    }

    #[wasm_bindgen]
    pub fn render(&mut self, client: &Client) -> Result<Vec<JsValue>, JsValue> {
        let mut new = Vec::with_capacity(self.len);
        for (n, e) in client.get_range(self.start .. u64::MAX).filter(|(_, e)| matches(&self.filter, e)).take(self.len) {
            let val = match self.cache.remove(&n) {
                Some(val) => val,
                None => self.produce.call2(&JsValue::null(), &serde_wasm_bindgen::to_value(&n).unwrap_or_default(), &wrap(e))?,
            };

            new.push((n, val));
        }
        self.cache.clear();
        self.cache.extend(new.iter().cloned());

        Ok(new.into_iter().map(|(_, v)| v).collect())
    }
}

fn wrap(e: BatchEntry<'_>) -> JsValue {
    serde_wasm_bindgen::to_value(&e).unwrap_or_default()
}
fn matches(filter: &Option<Filter>, e: &BatchEntry) -> bool {
    match filter {
        Some(f) => f.matches(e),
        None => true,
    }
}
