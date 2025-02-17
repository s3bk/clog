use std::{io::BufRead, time::Duration};

use clog::{shema::{BatchEntry, Builder}, Options, RequestEntry};

use std::hint::black_box;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode};

fn test_data() -> Vec<RequestEntry> {
    let file = std::fs::File::open("../artisan/user.log").unwrap();
    let mut entries = vec![];

    let mut reader = std::io::BufReader::new(file);
    let mut line = String::new();
    while let Ok(n) = reader.read_line(&mut line) {
        if n == 0 {
            break;
        }
        if let Ok(entry) = serde_json::from_str::<RequestEntry>(&line) {
            entries.push(entry);
        }
        line.clear();
    }
    entries
}


fn criterion_benchmark(c: &mut Criterion) {
    let data = test_data();

    c.bench_function("build", |b| {
        let mut builder = Builder::default();
        builder.reserve(data.len());
        b.iter(|| {
            for e in data.iter() {
                builder.add(e.into());
            }
        });
    });

    let mut builder = Builder::default();
    builder.reserve(data.len());
    for e in data.iter() {
        builder.add(e.into());
    }
    
    c.bench_function("clone", |b| {
        b.iter(|| builder.clone());
    });

    let data = builder.to_vec(&Default::default());

    c.bench_function("read", |b| {
        b.iter(|| {
            Builder::from_slice(&data)
        });
    });
    {
        let mut group = c.benchmark_group("brotli");
        group.sampling_mode(SamplingMode::Flat);
        group.measurement_time(Duration::from_secs(2));
        group.warm_up_time(Duration::from_nanos(200_000_000));

        for q in 1 ..= 11 {
            group.bench_with_input(BenchmarkId::from_parameter(q), &q, |b, &q| {
                let opt = Options {
                    brotli_level: q,
                    dict: b"",
                };
                b.iter(|| builder.to_vec(&opt));
            });
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

