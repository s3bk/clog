//! Turns a `ReqItem` into a fixed-length numeric feature vector.
//!
//! Three tiers of signal, in the order they appear in `FEATURE_NAMES`:
//!   1. Static per-request features (headers, UA, URI, method, TLS fp, ...)
//!   2. Fingerprint-consistency features (does this request "hang together"
//!      the way a real browser would?)
//!   3. Stateful per-IP behavioral features over a sliding window.
//!
//! With only ~1000 labels, feature quality matters far more than model
//! complexity, so this file is intentionally the bulk of the system.

use dashmap::DashMap;
use ndarray::{Array1, Array2};
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::{IpAddr, Ipv6Addr};
use std::sync::Arc;

use aho_corasick::AhoCorasick;

const HTTP: u16 = Protocol::Http as u16;
const HTTPS: u16 = Protocol::Https as u16;
use crate::types::{Protocol, ReqItem};

/// Human-readable names, in the exact order `extract()` pushes values.
/// Keep this in lockstep with the push order below — it's your audit trail
/// and it's what your GBDT/RF feature-importance output will reference.
pub const FEATURE_NAMES: [&str; 38] = [
    "header_count",
    "sec_fetch_header_count",
    "sec_ch_ua_present",
    "has_ua",
    "ua_len",
    "ua_claims_browser",
    "ua_known_bot_substring",
    "accept_present",
    "accept_language_present",
    "accept_language_is_wildcard",
    "accept_encoding_present",
    "connection_present",
    "missing_common_header_count",
    "header_order_exact_match_known_browser",
    "header_name_jaccard_nearest_profile",
    "tls_known_bot_fp",
    "tls_known_browser_fp",
    "tls_ua_mismatch",
    "host_header_mismatch",
    "uri_length",
    "uri_query_param_count",
    "uri_path_depth",
    "uri_suspicious_pattern",
    "method_get",
    "method_post",
    "method_other",
    "status_is_error",
    "body_present",
    "body_len_log1p",
    "port_is_common",
    "proto_is_https",
    "hour_sin",
    "hour_cos",
    "ip_req_rate_1m",
    "ip_req_rate_5m",
    "ip_unique_uris_5m",
    "ip_unique_uas_5m",
    "ip_static_asset_ratio_5m",
];

pub const N_FEATURES: usize = FEATURE_NAMES.len();

const WINDOW_SECS: u64 = 300; // 5 minutes
const MAX_EVENTS_PER_IP: usize = 1000; // memory bound for hot/abusive IPs

fn bf(b: bool) -> f64 {
    if b {
        1.0
    } else {
        0.0
    }
}

/// A known, curated header-order + header-name fingerprint for a real
/// browser family (Chrome/Firefox/Safari/Edge desktop+mobile). In
/// production this is loaded from a JSON file you maintain and refresh —
/// browsers change header sets across major versions — not hardcoded.
struct HeaderProfile {
    name: &'static str,
    order: Vec<&'static str>,
}

struct IpEvent {
    ts: u64,
    uri_hash: u64,
    ua_hash: u64,
    is_static_asset: bool,
}

struct IpWindow {
    events: VecDeque<IpEvent>,
}

pub struct FeatureExtractor {
    ip_windows: Arc<DashMap<Ipv6Addr, IpWindow>>,
    known_profiles: Vec<HeaderProfile>,
    bot_ua_matcher: AhoCorasick,
    known_bot_tls_fps: HashSet<[u8; 16]>,
    known_browser_tls_fps: HashSet<[u8; 16]>,
    common_headers: Vec<&'static str>,
}

impl FeatureExtractor {
    /// `bot_ua_substrings`, `bot_tls_fps`, `browser_tls_fps` are curated
    /// offline (bot UA library signatures; TLS JA3/JA4-style fingerprints
    /// observed from known scanners vs known browsers). Pass empty
    /// collections to start and grow them as your active-learning loop
    /// surfaces confirmed cases.
    pub fn new(
        bot_ua_substrings: &[&str],
        bot_tls_fps: HashSet<[u8; 16]>,
        browser_tls_fps: HashSet<[u8; 16]>,
    ) -> Self {
        let known_profiles = vec![
            HeaderProfile {
                name: "chrome_desktop",
                order: vec![
                    "host",
                    "connection",
                    "sec-ch-ua",
                    "sec-ch-ua-mobile",
                    "sec-ch-ua-platform",
                    "upgrade-insecure-requests",
                    "user-agent",
                    "accept",
                    "sec-fetch-site",
                    "sec-fetch-mode",
                    "sec-fetch-user",
                    "sec-fetch-dest",
                    "accept-encoding",
                    "accept-language",
                ],
            },
            HeaderProfile {
                name: "firefox_desktop",
                order: vec![
                    "host",
                    "user-agent",
                    "accept",
                    "accept-language",
                    "accept-encoding",
                    "connection",
                    "upgrade-insecure-requests",
                    "sec-fetch-dest",
                    "sec-fetch-mode",
                    "sec-fetch-site",
                    "sec-fetch-user",
                ],
            },
            HeaderProfile {
                name: "safari_desktop",
                order: vec![
                    "host",
                    "accept",
                    "sec-fetch-site",
                    "accept-encoding",
                    "accept-language",
                    "user-agent",
                    "connection",
                ],
            },
        ];

        let bot_ua_matcher = AhoCorasick::new(bot_ua_substrings)
            .expect("valid Aho-Corasick patterns");

        Self {
            ip_windows: Arc::new(DashMap::new()),
            known_profiles,
            bot_ua_matcher,
            known_bot_tls_fps: bot_tls_fps,
            known_browser_tls_fps: browser_tls_fps,
            common_headers: vec![
                "accept",
                "accept-language",
                "accept-encoding",
                "user-agent",
                "connection",
            ],
        }
    }

    pub fn extract_batch(&self, items: &[ReqItem]) -> Array2<f64> {
        let mut out = Array2::<f64>::zeros((items.len(), N_FEATURES));
        for (i, item) in items.iter().enumerate() {
            out.row_mut(i).assign(&self.extract(item));
        }
        out
    }

    pub fn extract(&self, item: &ReqItem) -> Array1<f64> {
        let headers_lower: Vec<(String, String)> = item
            .headers
            .iter()
            .map(|&(k, v)| (k.to_ascii_lowercase(), v.into()))
            .collect();
        let header_map: HashMap<&str, &str> = headers_lower
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let order: Vec<&str> = headers_lower.iter().map(|(k, _)| k.as_str()).collect();

        let mut f = Vec::with_capacity(N_FEATURES);

        // --- static structural ---
        f.push(item.headers.len() as f64);
        f.push(order.iter().filter(|h| h.starts_with("sec-fetch-")).count() as f64);
        f.push(bf(header_map.contains_key("sec-ch-ua")));

        let ua = header_map.get("user-agent").copied().unwrap_or("");
        f.push(bf(!ua.is_empty()));
        f.push(ua.len() as f64);
        f.push(bf(ua_claims_browser(ua)));
        f.push(bf(self.bot_ua_matcher.is_match(ua)));

        f.push(bf(header_map.contains_key("accept")));
        let accept_lang = header_map.get("accept-language").copied();
        f.push(bf(accept_lang.is_some()));
        f.push(bf(accept_lang == Some("*")));
        f.push(bf(header_map.contains_key("accept-encoding")));
        f.push(bf(header_map.contains_key("connection")));

        let missing = self
            .common_headers
            .iter()
            .filter(|h| !header_map.contains_key(*h))
            .count();
        f.push(missing as f64);

        // --- fingerprint consistency ---
        let (order_match, jaccard) = self.nearest_profile(&order);
        f.push(bf(order_match));
        f.push(jaccard);

        let tls_bot = self.known_bot_tls_fps.contains(&item.tls_fp);
        let tls_browser = self.known_browser_tls_fps.contains(&item.tls_fp);
        f.push(bf(tls_bot));
        f.push(bf(tls_browser));
        // TLS stack says "not a browser" but UA claims to be one — classic
        // header-spoofing tell (curl/requests/scrapy w/ a faked UA string).
        f.push(bf(ua_claims_browser(ua) && !tls_browser && item.proto == HTTPS));

        // Host header (if resent as a raw header) should match the parsed
        // `host` field a normal client/proxy produced.
        let host_hdr = header_map.get("host").copied();
        f.push(bf(host_hdr.is_some() && host_hdr != Some(item.host)));

        // --- URI / method / status / body ---
        f.push(item.uri.len() as f64);
        let qp = item
            .uri
            .split_once('?')
            .map(|(_, q)| q.split('&').filter(|s| !s.is_empty()).count())
            .unwrap_or(0);
        f.push(qp as f64);
        f.push(item.uri.matches('/').count() as f64);
        f.push(bf(is_suspicious_uri(&item.uri)));

        f.push(bf(item.method.eq_ignore_ascii_case("GET")));
        f.push(bf(item.method.eq_ignore_ascii_case("POST")));
        f.push(bf(
            !item.method.eq_ignore_ascii_case("GET") && !item.method.eq_ignore_ascii_case("POST")
        ));

        f.push(bf(item.status >= 400));

        let body_len = item.body.as_ref().map(|b| b.len()).unwrap_or(0);
        f.push(bf(body_len > 0));
        f.push(((body_len as f64) + 1.0).ln());

        f.push(bf(item.port == 80 || item.port == 443));
        f.push(bf(matches!(item.proto, HTTPS)));

        let hour = ((item.time / 3600) % 24) as f64;
        let angle = hour / 24.0 * std::f64::consts::TAU;
        f.push(angle.sin());
        f.push(angle.cos());

        // --- stateful, per-IP behavioral ---
        let (rate_1m, rate_5m, uniq_uris, uniq_uas, static_ratio) =
            self.update_and_read_ip_stats(item, is_static_asset(&item.uri));
        f.push(rate_1m);
        f.push(rate_5m);
        f.push(uniq_uris);
        f.push(uniq_uas);
        f.push(static_ratio);

        debug_assert_eq!(f.len(), N_FEATURES);
        Array1::from(f)
    }

    fn nearest_profile(&self, order: &[&str]) -> (bool, f64) {
        let obs: HashSet<&str> = order.iter().copied().collect();
        let mut best_jaccard = 0.0f64;
        let mut exact = false;

        for profile in &self.known_profiles {
            if profile.order == order {
                exact = true;
            }
            let known: HashSet<&str> = profile.order.iter().copied().collect();
            let inter = obs.intersection(&known).count() as f64;
            let union = obs.union(&known).count().max(1) as f64;
            best_jaccard = best_jaccard.max(inter / union);
        }
        (exact, best_jaccard)
    }

    /// Records this request into the IP's sliding window, prunes anything
    /// older than `WINDOW_SECS`, and returns the resulting stats. Called
    /// once per `extract()`, so behavioral features always include the
    /// current request.
    fn update_and_read_ip_stats(
        &self,
        item: &ReqItem,
        is_static: bool,
    ) -> (f64, f64, f64, f64, f64) {
        let mut entry = self
            .ip_windows
            .entry(item.ip)
            .or_insert_with(|| IpWindow {
                events: VecDeque::new(),
            });

        entry.events.push_back(IpEvent {
            ts: item.time,
            uri_hash: fnv1a(item.uri.as_bytes()),
            ua_hash: fnv1a(
                item.headers
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case("user-agent"))
                    .map(|&(_, v)| v)
                    .unwrap_or("")
                    .as_bytes(),
            ),
            is_static_asset: is_static,
        });

        let cutoff = item.time.saturating_sub(WINDOW_SECS);
        while entry
            .events
            .front()
            .map(|e| e.ts < cutoff)
            .unwrap_or(false)
        {
            entry.events.pop_front();
        }
        while entry.events.len() > MAX_EVENTS_PER_IP {
            entry.events.pop_front();
        }

        let one_min_cutoff = item.time.saturating_sub(60);
        let rate_1m = entry.events.iter().filter(|e| e.ts >= one_min_cutoff).count() as f64;
        let rate_5m = entry.events.len() as f64;

        let uniq_uris = entry
            .events
            .iter()
            .map(|e| e.uri_hash)
            .collect::<HashSet<_>>()
            .len() as f64;
        let uniq_uas = entry
            .events
            .iter()
            .map(|e| e.ua_hash)
            .collect::<HashSet<_>>()
            .len() as f64;

        let static_count = entry.events.iter().filter(|e| e.is_static_asset).count() as f64;
        let static_ratio = static_count / entry.events.len().max(1) as f64;

        (rate_1m, rate_5m, uniq_uris, uniq_uas, static_ratio)
    }
}

fn ua_claims_browser(ua: &str) -> bool {
    let ua_l = ua.to_ascii_lowercase();
    ["chrome", "firefox", "safari", "edg/", "edge"]
        .iter()
        .any(|s| ua_l.contains(s))
}

fn is_suspicious_uri(uri: &str) -> bool {
    const PATTERNS: [&str; 8] = [
        "wp-login", "wp-admin", ".env", "/.git", "phpmyadmin", "xmlrpc.php", "/etc/passwd", "..%2f",
    ];
    let l = uri.to_ascii_lowercase();
    PATTERNS.iter().any(|p| l.contains(p))
}

fn is_static_asset(uri: &str) -> bool {
    const EXT: [&str; 9] = [
        ".css", ".js", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".woff", ".ico",
    ];
    let l = uri.split('?').next().unwrap_or(uri).to_ascii_lowercase();
    EXT.iter().any(|e| l.ends_with(e))
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in bytes {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}
