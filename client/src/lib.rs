use std::{collections::{BTreeMap, HashMap, VecDeque}, net::Ipv6Addr, ops::Range, str::from_utf8_unchecked, sync::Arc};

use js_sys::{BigInt, Function, JsString, Object, Uint8Array};
use time::OffsetDateTime;
use wasm_bindgen::{prelude::wasm_bindgen, JsCast, JsValue};
use web_sys::{BinaryType, Event, MessageEvent, WebSocket};
use clog_core::{filter::{Filter, FilterCtx}, shema, BatchHeader, PacketType, SyncHeader};
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

    requested_start: u64,

    reconnecting: bool,
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
            requested_start: 0,
            websocket,
            reconnecting: false,
        }
    }
    pub fn reconnect(&mut self, websocket: WebSocket) {
        self.websocket = websocket;
        self.reconnecting = true;
    }
    fn send(&self, msg: ClientMessage) {
        let data = postcard::to_stdvec(&msg).unwrap();
        self.websocket.send_with_u8_array(&data);
    }
    fn request_more(&mut self, start: u64) {
        if start < self.requested_start {
            let start = start.min(self.requested_start.saturating_sub(1000));
            debug!("requesting range {}..{}", start, self.requested_start);
            self.send(ClientMessage::FetchRange { start, end: self.requested_start });
            self.requested_start = start;
        }
    }
    fn maybe_need_more(&mut self, start: u64) {
        self.request_more(start.saturating_sub(1000));
    }
    pub fn on_open(&mut self, e: Event) {
        self.send(ClientMessage::SubScribeWithBacklog { backlog: 1000 });
    }
    pub fn on_message(&mut self, event: MessageEvent) -> Option<PacketRange> {
        let data = event.data();
        let data = Uint8Array::new(&data);
        let data = data.to_vec();
        self.handle_packet(&data).map(|r| PacketRange { start: r.start, end: r.end })
    }
    fn get_entry(&self, n: u64) -> Option<BatchEntry> {
        if n >= self.current_start {
            if let Some(val) = self.current.get((n - self.current_start) as usize) {
                return Some(val);
            }
        }
        if let Some((&start, chunk)) = self.entries.range(..=n).rev().next() {
            if start <= n && start + chunk.len() as u64 > n {
                let val = chunk.get((n - start) as usize);
                return val;
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
                debug!("batch");
                let (header, rest) = postcard::take_from_bytes::<BatchHeader>(rest).ok()?;
                let builder = match Builder::from_slice(rest) {
                    Ok(b) => b,
                    Err(e) => {
                        debug!("batch {}: error: {e:?}", header.start);
                        return None;
                    }
                };
                let range = header.start .. header.start + builder.len() as u64;
                if header.start < self.requested_start {
                    self.requested_start = header.start;
                }
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
                if let Ok(info) = postcard::from_bytes::<SyncHeader>(rest) {
                    self.current_start = info.start;
                    self.requested_start = info.first_backlog;
                    debug!("SYNC to {}, backlog at {}", info.start, info.first_backlog);

                    if self.reconnecting {
                        let end = self.end();
                        self.send(ClientMessage::FetchRange { start: end, end: self.requested_start });
                    }
                }
                None
            }
            PacketType::ServerMsg => {
                if let Ok((msg, _)) = postcard::take_from_bytes::<ServerMessage>(rest) {
                    match msg {
                        ServerMessage::Detached | ServerMessage::NotAttached => {
                            self.send(ClientMessage::SubScribeWithBacklog { backlog: 1000 });
                        }
                        ServerMessage::Error { msg } => {
                            debug!("server error: {msg}");
                        }
                    }
                }
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
    // returns true if the end in that direction was reached
    pub fn scroll_by(&mut self, client: &mut Client, by: i32) -> bool {
        if by > 0 {
            let max = client.end().saturating_sub(self.len as u64);
            let new_start = self.start + by as u64;
            if new_start >= max {
                self.start = max;
                true
            } else {
                self.start = new_start;
                false
            }
        } else {
            self.start = self.start.saturating_sub((-by) as u64);
            client.maybe_need_more(self.start);
            self.start == 0
        }
    }
    pub fn scroll_to(&mut self, pos: u64) {
        self.start = pos;
    }
    pub fn scroll_to_end(&mut self, client: &Client) {
        self.start = client.end().saturating_sub(self.len as u64);
    }
    pub fn pos(&self) -> u64 {
        self.start
    }
    fn produce(&self, n: u64, e: BatchEntry<'_>) -> Result<JsValue, JsValue> {
        self.produce.call2(&JsValue::null(), &bigint(n), &wrap(e))
    }
    pub fn render(&mut self, client: &Client) -> Result<Vec<JsValue>, JsValue> {
        if self.start > self.current_start {
            // trim some from the front
            let offset = (self.start - self.current_start) as usize;
            if offset >= self.current.len() {
                self.current.clear();
            } else {
                self.current.drain(..offset);
            }
        }
        if self.start < self.current_start {
            // trim from the end and add to the front
            let offset = (self.current_start - self.start) as usize;
            let end = self.current.len().saturating_sub(offset);
            self.current.drain(end..);
            assert!(self.len >= self.current.len());
            
            // the remaining number of entries
            let max_len = (client.end().saturating_sub(self.current_start)) as usize;

            // don't try to add more than could be added
            let i1 = self.len.min(max_len).saturating_sub(self.current.len());
            for i in (0 .. i1).rev() {
                let n = self.start + i as u64;
                if let Some(e) = client.get_entry(n) {
                    let val = self.produce(n, e)?;
                    self.current.push_front(val);
                }
            }
        }

        let i0 = self.current.len();
        for i in i0 .. self.len {
            let n = self.start + i as u64;
            if let Some(e) = client.get_entry(n) {
                let val = self.produce(n, e)?;
                self.current.push_back(val);
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
    positions: VecDeque<u64>,

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
            positions: VecDeque::with_capacity(len),
        }
    }

    pub fn pos(&self) -> u64 {
        self.start
    }
    pub fn scroll_to(&mut self, pos: u64) {
        self.start = pos;
    }
    pub fn scroll_to_end(&mut self, client: &Client) {
        let ctx = FilterCtx::new();
        let filter = &self.filter;
        let matches = |&(n, ref e): &(u64, BatchEntry)| matches(filter, &ctx, e);

        let end = self.positions.back().cloned().unwrap_or(self.start);
        for (pos, _) in client.get_range(end+1 .. u64::MAX).filter(matches) {
            if self.positions.len() >= self.len {
                self.positions.pop_front();
            }
            self.positions.push_back(pos);
        }
        if self.len > self.positions.len() {
            for (p, _) in client.get_range(0 .. self.start).rev().filter(matches).take(self.len - self.positions.len()) {
                self.positions.push_front(p);
            }
        }
        if let Some(&pos) = self.positions.front() {
            self.start = pos;
        }
    }
    pub fn scroll_by(&mut self, client: &mut Client, by: isize) -> bool {
        let ctx = FilterCtx::new();
        let filter = &self.filter;
        let matches = |&(n, ref e): &(u64, BatchEntry)| matches(filter, &ctx, e);

        if by > 0 {
            let end = self.positions.back().cloned().unwrap_or(self.start);
            let mut take = by as usize;
            for (pos, _) in client.get_range(end+1 .. u64::MAX).filter(matches) {
                if take == 0 {
                    break;
                }
                take -= 1;

                if self.positions.len() >= self.len {
                    self.positions.pop_front();
                }
                self.positions.push_back(pos);
            }
            if let Some(&pos) = self.positions.front() {
                self.start = pos;
            }
            take > 0
        } else {
            let pos = client.get_range(0 .. self.start).rev().filter(matches).take((-by) as usize).last().map(|(pos, _)| pos).unwrap_or(0);
            self.start = pos;
            client.maybe_need_more(self.start);
            self.start == 0
        }
    }

    pub fn set_filter(&mut self, val: JsValue) -> Result<(), JsValue> {
        if val.is_null() {
            self.filter = None;
        } else if let Some(s) = val.as_string() {
            self.filter = Some(Filter::parse(&s).map_err(|e| JsValue::from_str(&e.to_string()))?);
        } else {
            return Err(JsValue::from_str("expects a string or null"));
        }
        Ok(())
    }

    #[wasm_bindgen]
    pub fn render(&mut self, client: &Client) -> Result<Vec<JsValue>, JsValue> {
        let ctx = FilterCtx::new();

        let mut new = Vec::with_capacity(self.len);
        self.positions.clear();
        for (n, e) in client.get_range(self.start .. u64::MAX).filter(|(_, e)| matches(&self.filter, &ctx, e)).take(self.len) {
            let val = match self.cache.remove(&n) {
                Some(val) => val,
                None => self.produce.call2(&JsValue::null(), &bigint(n), &wrap(e))?,
            };

            new.push(val);
            self.positions.push_back(n);
        }
        self.cache.clear();
        self.cache.extend(self.positions.iter().zip(&new).map(|(&n, v)| (n, v.clone())));

        Ok(new)
    }
}

#[wasm_bindgen(module="/src/lib.js")]
extern "C" {
    pub fn make_entry(status: u16, method: &str, uri: &str, ua: Option<&str>, referer: Option<&str>, ip: &str, port: u16, time: &str, body: Option<&[u8]>) -> JsValue;
}

struct ArrayStr<'a> {
    data: &'a mut [u8],
    len: usize
}
impl<'a> ArrayStr<'a> {
    pub fn new(data: &'a mut [u8]) -> Self {
        ArrayStr { data, len: 0 }
    }
    pub fn as_str(&self) -> &str {
        unsafe {
            std::str::from_utf8_unchecked(&self.data[..self.len])
        }
    }
}
impl<'a> std::fmt::Write for ArrayStr<'a> {
    fn write_str(&mut self, s: &str) -> Result<(), std::fmt::Error> {
        if let Some(part) = self.data.get_mut(self.len..self.len + s.len()) {
            part.copy_from_slice(s.as_bytes());
            self.len += s.len();
        }
        Ok(())
    }
    fn write_char(&mut self, c: char) -> Result<(), std::fmt::Error> {
        let c_len = c.len_utf8();
        if let Some(dst) = self.data.get_mut(self.len .. self.len + c_len) {
            c.encode_utf8(dst);
            self.len += c_len;
        }
        Ok(())
    }
}

fn wrap(e: BatchEntry<'_>) -> JsValue {
    let mut time_buf = [0; 20];
    let mut ip_buf = [0; 40];

    let time = format_time(&mut time_buf, e.time);
    let ip = format_ip(&mut ip_buf, e.ip);
    make_entry(
        e.status,
        e.method,
        e.uri,
        e.ua,
        e.referer,
        ip.as_str(),
        e.port,
        time.as_str(),
        e.body
    )
}

fn matches(filter: &Option<Filter>, ctx: &FilterCtx, e: &BatchEntry) -> bool {
    match filter {
        Some(f) => f.matches(ctx, e),
        None => true,
    }
}

fn format_time(buf: &mut [u8; 20], n: u64) -> ArrayStr {
    use std::fmt::Write;
    let mut s = ArrayStr::new(buf);
    match OffsetDateTime::from_unix_timestamp(n as i64) {
        Ok(t) => write!(s, "{:04}-{:02}-{:02} {:02}:{:02}:{:02}", t.year(), u8::from(t.month()), t.day(), t.hour(), t.minute(), t.second()).unwrap(),
        Err(_) => write!(s, "Invalid time {n}").unwrap()
    }
    s
}
fn format_ip(buf: &mut [u8; 40], ip: Ipv6Addr) -> ArrayStr {
    use std::fmt::Write;

    let mut s = ArrayStr::new(buf);
    if let Some(ipv4) = ip.to_ipv4_mapped() {
        write!(s, "{ipv4}").unwrap();
    } else {
        write!(s, "{ip}").unwrap();
    }
    s
}

fn bigint(n: u64) -> JsValue {
    BigInt::from(n).unchecked_into()
}

#[wasm_bindgen]
pub fn hex_view(data: &[u8]) -> String {
    use hexplay::HexViewBuilder;
    HexViewBuilder::new(&data)
        .row_width(16)
        .finish().to_string()
}
