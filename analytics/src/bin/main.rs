use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::PathBuf;

use clog_collector::decode_batch;
use clog_core::shema::{Builder, Shema};
use ndarray::{Array1, Array2, s};

use analytics::features::{FeatureExtractor, N_FEATURES};
use analytics::inference::BotClassifier;
use analytics::model::{BotForest, ForestConfig};
use analytics::types::{Protocol, ReqItem};
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr, Map};

#[serde_as]
#[derive(Deserialize)]
struct Classifications(#[serde_as(as = "Map<DisplayFromStr, _>")] Vec<(u64, bool)>);

struct Loader {
    blocks: BTreeMap<u64, Option<Builder>>,
}
impl Loader {
    fn init() -> Self {
        let mut blocks = BTreeMap::new();
        for e in std::fs::read_dir("../logs").unwrap().filter_map(|r| r.ok()) {
            if e.file_type().map_or(false, |t| t.is_file()) {
                let n: Option<u64> = e.file_name().to_str().and_then(|s| s.strip_prefix("block-")).and_then(|s| s.strip_suffix(".clog")).and_then(|s| s.parse().ok());
                if let Some(n) = n {
                    blocks.insert(n, None);
                }
            }
        }
        Loader { blocks }
    }
    fn load(&mut self, n: u64) -> Option<ReqItem<'_>> {
        let (&start, opt) = self.blocks.range_mut(..n).rev().next()?;
        let block = opt.get_or_insert_with(|| {
            let path  = format!("../logs/block-{}.clog", start);
            let data = std::fs::read(&path).unwrap();
            let (start2, block) = decode_batch(&data).unwrap();
            assert_eq!(start, start2);

            block
        });
        let idx = (n - start) as usize;
        block.get(idx)
    }
}

fn sample_request() -> ReqItem<'static> {
    ReqItem {
        ua: None,
        referer: None,
        status: 200,
        method: "GET".into(),
        uri: "/".into(),
        ip: Ipv4Addr::new(100, 30, 240, 108).to_ipv6_mapped(),
        port: 53712,
        time: 1_786_123_989,
        body: None,
        headers: vec![
            ("host".into(), "qdat.net".into()),
            ("connection".into(), "keep-alive".into()),
            (
                "user-agent".into(),
                "Mozilla/5.0 (compatible; AntibotDetector/1.0; +https://proxybase.xyz)".into(),
            ),
            (
                "accept".into(),
                "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8".into(),
            ),
            ("accept-language".into(), "*".into()),
            ("sec-fetch-mode".into(), "cors".into()),
            ("pragma".into(), "no-cache".into()),
            ("cache-control".into(), "no-cache".into()),
            ("accept-encoding".into(), "br, gzip, deflate".into()),
        ],
        host: "qdat.net".into(),
        proto: Protocol::Https as u16,
        location: "FR".into(),
        tls_fp: [0u8; 16],
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source: PathBuf = std::env::args_os().nth(1).unwrap().into();

    let classifications: Classifications = serde_json::from_reader(File::open(&source).unwrap()).unwrap();

    let mut loader = Loader::init();
    // In production: load these from curated JSON assets that your
    // active-learning loop keeps growing over time.
    let bot_ua_substrings = ["bot", "spider", "crawl", "AntibotDetector", "python-requests"];
    let known_bot_tls_fps: HashSet<[u8; 16]> = HashSet::new();
    let known_browser_tls_fps: HashSet<[u8; 16]> = HashSet::new();

    let extractor = FeatureExtractor::new(&bot_ua_substrings, known_bot_tls_fps, known_browser_tls_fps);

    let mut x = Array2::<f64>::zeros((classifications.0.len(), N_FEATURES));
    let mut x_rows = x.rows_mut().into_iter();
    let mut y = Array1::<usize>::zeros((classifications.0.len(),));
    let mut y_rows = y.iter_mut();

    let mut n_samples = 0usize;
    for &(n, c) in classifications.0.iter() {
        if let Some(sample) = loader.load(n) {
            x_rows.next().unwrap().assign(&extractor.extract(&sample));
            *y_rows.next().unwrap() = c as usize;
            n_samples += 1;
        }
    }

    println!("{n_samples} samples");
    let forest = BotForest::fit(x.slice(s![..n_samples, ..]).view(), y.slice(s!(..n_samples)).view(), &ForestConfig::default())?;
    forest.save("bot_forest.bin")?;

    // --- inference ---
    let extractor_for_inference =
        FeatureExtractor::new(&bot_ua_substrings, HashSet::new(), HashSet::new());
    let classifier = BotClassifier::load("bot_forest.bin", extractor_for_inference)?
        .with_thresholds(0.3, 0.7);

    for i in 1_500_000 .. 1_600_000 {
        if let Some(req) = loader.load(i) {
            let (score, verdict) = classifier.classify(&req);
            println!("{i} {} bot_score={:.3} verdict={:?}", req.uri, score, verdict);
        }
    }


    Ok(())
}
