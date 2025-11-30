use std::borrow::Cow;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::num::ParseIntError;

use lalrpop_util::{lalrpop_mod, ParseError};
use regex::Regex;
use serde::{Deserialize, Deserializer};
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};
use crate::Protocol;
use crate::shema::BatchEntry;

lalrpop_mod!(grammar);

#[derive(Debug, Deserialize)]
pub enum StringFilter {
    Equals(String),
    Similar(String, usize),
    Prefix(String),
    Suffix(String),
    Contains(String),
    Regex(#[serde(deserialize_with="deser_regex")] Regex),
}
impl PartialEq for StringFilter {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (StringFilter::Equals(a), StringFilter::Equals(b)) => a == b,
            (StringFilter::Similar(a, an), StringFilter::Similar(b, bn)) => a == b && an == bn,
            (StringFilter::Prefix(a), StringFilter::Prefix(b)) => a == b,
            (StringFilter::Contains(a), StringFilter::Contains(b)) => a == b,
            (StringFilter::Regex(a), StringFilter::Regex(b)) => a.as_str() == b.as_str(),
            _ => false
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub enum NumberFilter<T> {
    Equals(T),
    Range(T, T),
}
impl<T: PartialOrd + Copy> NumberFilter<T> {
    pub fn matches(&self, n: T) -> bool {
        match *self {
            NumberFilter::Equals(m) => m == n,
            NumberFilter::Range(a, b) => (a..b).contains(&n)
        }
    }
}
impl StringFilter {
    pub fn matches(&self, s: &str) -> bool {
        match self {
            Self::Equals(t) => s == t,
            Self::Contains(t) => s.contains(t),
            Self::Prefix(t) => s.starts_with(t),
            Self::Suffix(t) => s.ends_with(t),
            Self::Regex(r) => r.is_match(s),
            &Self::Similar(ref t, n) => strsim::levenshtein(s, t) <= n,
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct IpBlock {
    bits: u8,
    mask: u8,
}
impl IpBlock {
    fn any() -> Self {
        IpBlock { bits: 0, mask: 0 }
    }
    fn byte(n: u8) -> Self {
        IpBlock { bits: n, mask: 255 }
    }
}
#[derive(Debug, Deserialize, PartialEq)]
pub struct IpFilter {
    bits: u128,
    mask: u128,
}
impl IpFilter {
    pub fn ipv4(a: IpBlock, b: IpBlock, c: IpBlock, d: IpBlock) -> Self {
        let bytes = [  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,255,255,a.bits,b.bits,c.bits,d.bits];
        let mask =  [255,255,255,255,255,255,255,255,255,255,255,255,a.mask,b.mask,c.mask,d.mask];
        IpFilter {
            bits: u128::from_be_bytes(bytes),
            mask: u128::from_be_bytes(mask)
        }
    }
    pub fn matches(&self, ip: Ipv6Addr) -> bool {
        (ip.to_bits() ^ self.bits) & self.mask == 0
    }
}

pub struct FilterCtx {
    pub now: u64
}
impl FilterCtx {
    pub fn new() -> Self {
        FilterCtx {
            now: OffsetDateTime::now_utc().unix_timestamp() as u64
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct TimeFilter {
    pub start: Option<TimeSpec>,
    pub end: Option<TimeSpec>
}

impl TimeFilter {
    fn day(d: Date) -> Self {
        TimeFilter { start: Some(timestamp(d, Time::MIDNIGHT)), end: d.next_day().map(|e| timestamp(e, Time::MIDNIGHT)) }
    }
    fn after(t: TimeSpec) -> Self {
        TimeFilter { start: Some(t), end: None }
    }
    fn before(t: TimeSpec) -> Self {
        TimeFilter { start: None, end: Some(t) }
    }
    fn between(a: TimeSpec, b: TimeSpec) -> Self {
        TimeFilter { start: Some(a), end: Some(b) }
    }
    pub fn matches(&self, ctx: &FilterCtx, t: u64) -> bool {
        let c1 = match self.start {
            None => true,
            Some(TimeSpec::Absolute(start)) => t >= start,
            Some(TimeSpec::Relative(dt)) => t >= ctx.now + (dt as u64),
        };
        let c2 = match self.end {
            None => true,
            Some(TimeSpec::Absolute(end)) => t < end,
            Some(TimeSpec::Relative(dt)) => t < ctx.now + (dt as u64)
        };
        c1 & c2
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub enum TimeSpec {
    Relative(i64),
    Absolute(u64),
}

#[derive(Copy, Clone, Debug, Deserialize, PartialEq)]
#[repr(u16)]
pub enum ProtoFilter {
    Http = Protocol::Http as u16,
    Https = Protocol::Https as u16,
}
impl ProtoFilter {
    pub fn matches(&self, proto: u16) -> bool {
        proto == Protocol::Unknown as u16 || *self as u16 == proto
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct HeaderFilter {
    header: String,
    filter: StringFilter
}
impl HeaderFilter {
    pub fn new(header: &str, filter: StringFilter) -> Self {
        HeaderFilter { header: header.to_ascii_lowercase(), filter }
    }
    pub fn matches(&self, headers: &[(&str, &str)]) -> bool {
        headers.iter().any(|&(key, val)| key == self.header && self.filter.matches(val))
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub enum FieldFilter {
    Status(NumberFilter<u16>),
    Method(StringFilter),
    Uri(StringFilter),
    Ip(IpFilter),
    Port(NumberFilter<u16>),
    Time(TimeFilter),
    Host(StringFilter),
    Proto(ProtoFilter),
    Header(HeaderFilter),
}

#[derive(Debug, Deserialize, PartialEq)]
pub enum Combinations {
    Not(Box<Filter>),
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Xor(Vec<Filter>),
}
use crate::filter::grammar::Token;

#[derive(Debug, Deserialize, PartialEq)]
#[serde(untagged)] 
pub enum Filter {
    Field(FieldFilter),
    Combination(Combinations)
}
impl Filter {
    pub fn matches(&self, ctx: &FilterCtx, entry: &BatchEntry) -> bool {
        match self {
            Filter::Field(f) => match f {
                FieldFilter::Port(n) => n.matches(entry.port),
                FieldFilter::Method(f) => f.matches(entry.method),
                FieldFilter::Status(n) => n.matches(entry.status),
                FieldFilter::Uri(s) => s.matches(entry.uri),
                FieldFilter::Ip(i) => i.matches(entry.ip),
                FieldFilter::Time(f) => f.matches(ctx, entry.time),
                FieldFilter::Host(f) => f.matches(entry.host),
                FieldFilter::Proto(f) => f.matches(entry.proto),
                FieldFilter::Header(f) => f.matches(&entry.headers),
            }
            Filter::Combination(c) => match c {
                Combinations::Not(f) => !f.matches(ctx, entry),
                Combinations::And(v) => v.iter().all(|f| f.matches(ctx, entry)),
                Combinations::Or(v) => v.iter().any(|f| f.matches(ctx, entry)),
                Combinations::Xor(v) => v.iter().fold(false, |b, f| b ^ f.matches(ctx, entry)),
            }
        }
    }
    pub fn parse(s: &str) -> Result<Self, ParseError<usize, Token, FilterParseError>> {
        grammar::FilterRootParser::new().parse(s)
    }
}

fn deser_regex<'de, D>(deserializer: D) -> Result<Regex, D::Error> where D: Deserializer<'de> {
    let s: Cow<str> = Cow::deserialize(deserializer)?;
    Regex::new(&s).map_err(serde::de::Error::custom)
}

fn apply_string_escapes(code: &str, idx0: usize) -> Result<String, lalrpop_util::ParseError<usize, Token, FilterParseError>> {
    if !code.contains('\\') {
        Ok(code.into())
    } else {
        let mut iter = code.char_indices();
        let mut text = String::new();
        while let Some((_, mut ch)) = iter.next() {
            if ch == '\\' {
                // The parser should never have accepted an ill-formed string
                // literal, so we know it can't end in a backslash.
                let (offset, next_ch) = iter.next().unwrap();
                ch = match next_ch {
                    '\\' | '\"' => next_ch,
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    _ => {
                        return Err(ParseError::InvalidToken { location: idx0 + offset });
                    }
                }
            }
            text.push(ch);
        }
        Ok(text.into())
    }
}

fn parse_date(year: u16, month: u8, day: u8) -> Result<Date, lalrpop_util::ParseError<usize, Token<'static>, FilterParseError>> {
    let month = month.try_into().map_err(|_| ParseError::User { error: FilterParseError::Date })?;
    Date::from_calendar_date(year as i32, month, day).map_err(|_| ParseError::User { error: FilterParseError::Date })
}
fn parse_time(hour: u8, minute: u8, second: u8) -> Result<Time, lalrpop_util::ParseError<usize, Token<'static>, FilterParseError>> {
    Time::from_hms(hour, minute, second).map_err(|_| ParseError::User { error: FilterParseError::Date })
}

fn timestamp(date: Date, time: Time) -> TimeSpec {
    TimeSpec::Absolute(PrimitiveDateTime::new(date, time).assume_utc().unix_timestamp() as u64)
}

fn join<T>(mut v: Vec<T>, other: T) -> Vec<T> {
    v.push(other);
    v
}


#[derive(Debug, PartialEq)]
pub enum FilterParseError {
    Regex(regex::Error),
    ParseInt(ParseIntError),
    Date,
}
impl std::fmt::Display for FilterParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FilterParseError::Regex(e) => write!(f, "Failed to parse Regex: {e}"),
            FilterParseError::ParseInt(e) => write!(f, "Integer out of range: {e}"),
            FilterParseError::Date => write!(f, "Invalid date"),
        }
    }
}

#[test]
fn test_filter_parser() {
    assert_eq!(Filter::parse("port 80"), Ok(Filter::Field(FieldFilter::Port(NumberFilter::Equals(80)))));
    assert_eq!(Filter::parse("port 80 & uri /api & port 100"), Ok(Filter::Combination(Combinations::And(vec![
        Filter::Field(FieldFilter::Port(NumberFilter::Equals(80))),
        Filter::Field(FieldFilter::Uri(StringFilter::Equals("/api".into()))),
        Filter::Field(FieldFilter::Port(NumberFilter::Equals(100)))
    ]))));
    assert_eq!(Filter::parse("uri /api/ *"), Ok(Filter::Field(FieldFilter::Uri(StringFilter::Prefix("/api/".into())))));
    assert_eq!(Filter::parse(r#"port 80 .. 100 & uri "/api/"*"#), Ok(Filter::Combination(Combinations::And(vec![
        Filter::Field(FieldFilter::Port(NumberFilter::Range(80, 100))), Filter::Field(FieldFilter::Uri(StringFilter::Prefix("/api/".into())))    
    ]))));
}
#[test]
fn test_lit_parser() {
    use grammar::{LitParser, SimpleLitParser, StrParser};
    assert_eq!(SimpleLitParser::new().parse("api"), Ok("api".into()));
    assert_eq!(StrParser::new().parse("\"api\""), Ok("api".into()));
    assert_eq!(LitParser::new().parse("/api"), Ok("/api".into()));
    assert_eq!(LitParser::new().parse("/api?foo=bar+baz&arg"), Ok("/api?foo=bar+baz&arg".into()));
}

#[test]
fn test_regex() {
    use grammar::RegexParser;
    assert_eq!(RegexParser::new().parse(r##"r"[0-1a-e]+""##).unwrap().as_str(), "[0-1a-e]+");
}
