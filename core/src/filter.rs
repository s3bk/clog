use std::borrow::Cow;

use lalrpop_util::{lalrpop_mod, ParseError};
use regex::Regex;
use serde::{Deserialize, Deserializer};
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
pub enum FieldFilter {
    Port(NumberFilter<u16>),
    UserAgent(StringFilter),
    Uri(StringFilter),
    Status(NumberFilter<u16>),
}

#[derive(Debug, Deserialize, PartialEq)]
pub enum Combinations {
    Not(Filter),
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Xor(Vec<Filter>),
}
use crate::filter::grammar::Token;

#[derive(Debug, Deserialize, PartialEq)]
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
    pub fn parse(s: &str) -> Result<Self, ParseError<usize, Token, FilterParseError>> {
        grammar::FilterRootParser::new().parse(s)
    }
}

fn deser_regex<'de, D>(deserializer: D) -> Result<Regex, D::Error> where D: Deserializer<'de> {
    let s: Cow<str> = Cow::deserialize(deserializer)?;
    Regex::new(&s).map_err(serde::de::Error::custom)
}

pub fn apply_string_escapes(code: &str, idx0: usize) -> Result<String, lalrpop_util::ParseError<usize, Token, FilterParseError>> {
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

#[derive(Debug, PartialEq)]
pub enum FilterParseError {
    Regex(regex::Error),
}
impl std::fmt::Display for FilterParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FilterParseError::Regex(e) => write!(f, "Failed to parse Regex: {e}"),
        }
    }
}

#[test]
fn test_filter_parser() {
    assert_eq!(Filter::parse("port 80"), Ok(Filter::Field(FieldFilter::Port(NumberFilter::Equals(80)))));
    assert_eq!(Filter::parse("uri /api/ *"), Ok(Filter::Field(FieldFilter::Uri(StringFilter::Prefix("/api/".into())))));
    assert_eq!(Filter::parse(r#"port 80..100 & uri "/api/"*"#), Ok(Filter::Combination(Box::new(Combinations::And(vec![
        Filter::Field(FieldFilter::Port(NumberFilter::Range(80, 100))), Filter::Field(FieldFilter::Uri(StringFilter::Prefix("/api/".into())))
    ])))));
}
#[test]
fn test_lit_parser() {
    use grammar::{LitParser, SimpleLitParser, StrParser};
    assert_eq!(SimpleLitParser::new().parse("api"), Ok("api".into()));
    assert_eq!(StrParser::new().parse("\"api\""), Ok("api".into()));
    assert_eq!(LitParser::new().parse("/api"), Ok("/api".into()));
    assert_eq!(LitParser::new().parse("/api?foo=bar+baz&arg"), Ok("/api?foo=bar+baz&arg".into()));
}
