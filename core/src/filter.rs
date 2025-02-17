use std::borrow::Cow;

use regex::Regex;
use serde::{Deserialize, Deserializer};
use crate::shema::BatchEntry;

#[derive(Deserialize)]
pub enum StringFilter {
    Equals(String),
    Similar(String, usize),
    Prefix(String),
    Contains(String),
    Regex(#[serde(deserialize_with="deser_regex")] Regex),
}
#[derive(Deserialize)]
pub enum NumberFilter<T> {
    Equals(T),
    Range(T, T)
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
            Self::Regex(r) => r.is_match(s),
            &Self::Similar(ref t, n) => strsim::levenshtein(s, t) <= n,
        }
    }
}
#[derive(Deserialize)]
pub enum FieldFilter {
    Port(NumberFilter<u16>),
    UserAgent(StringFilter),
    Uri(StringFilter),
    Status(NumberFilter<u16>),
}

#[derive(Deserialize)]
pub enum Combinations {
    Not(Filter),
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Xor(Vec<Filter>),
}

#[derive(Deserialize)]
#[serde(untagged)] 
pub enum Filter {
    Field(FieldFilter),
    Combination(Box<Combinations>)
}
impl Filter {
    pub fn matches(&self, entry: &BatchEntry) -> bool {
        match self {
            Filter::Field(f) => match f {
                FieldFilter::Port(n) => n.matches(entry.port),
                FieldFilter::Status(n) => n.matches(entry.status),
                FieldFilter::Uri(s) => s.matches(entry.uri),
                FieldFilter::UserAgent(s) => s.matches(entry.ua.unwrap_or_default()),
            }
            Filter::Combination(c) => match &**c {
                Combinations::Not(f) => !f.matches(entry),
                Combinations::And(v) => v.iter().all(|f| f.matches(entry)),
                Combinations::Or(v) => v.iter().any(|f| f.matches(entry)),
                Combinations::Xor(v) => v.iter().fold(false, |b, f| b ^ f.matches(entry)),
            }
        }
    }
}

fn deser_regex<'de, D>(deserializer: D) -> Result<Regex, D::Error> where D: Deserializer<'de> {
    let s: Cow<str> = Cow::deserialize(deserializer)?;
    Regex::new(&s).map_err(serde::de::Error::custom)
}
