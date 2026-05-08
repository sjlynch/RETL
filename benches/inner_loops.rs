//! Criterion microbenchmarks for the three hot inner-loop primitives that the
//! recent T6 ahash + drop-per-line-to_lowercase work touched. Defends future
//! perf changes against silent regressions.
//!
//! Run via:
//!   cargo bench --bench inner_loops -- --save-baseline pre   (before change)
//!   cargo bench --bench inner_loops -- --baseline pre        (after change)

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use retl::{
    for_each_line_cfg, matches_minimal, parse_minimal, rewrite_human_timestamps_bytes,
    MinimalRecord, QuerySpec,
};
use serde_json::Value;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::OnceLock;

/// Single fixture file under benches/data/. Embedded at compile time so the
/// bench binary doesn't depend on cwd.
const SAMPLE_JSONL: &str = include_str!("data/sample.jsonl");

/// Replication factor when materializing the .zst fixture. Keeps the fixture
/// big enough that streaming dominates per-iter overhead but small enough to
/// stay snappy (~tens of MB compressed).
const ZST_REPLICAS: usize = 1000;

fn lines() -> &'static Vec<&'static str> {
    static LINES: OnceLock<Vec<&'static str>> = OnceLock::new();
    LINES.get_or_init(|| {
        SAMPLE_JSONL
            .lines()
            .filter(|l| !l.trim().is_empty())
            .collect()
    })
}

fn parsed_records() -> &'static Vec<MinimalRecord> {
    static RECS: OnceLock<Vec<MinimalRecord>> = OnceLock::new();
    RECS.get_or_init(|| {
        lines()
            .iter()
            .map(|l| parse_minimal(l).expect("fixture line parses"))
            .collect()
    })
}

/// Lines from the fixture: every line in sample.jsonl already contains all
/// three timestamp keys (created_utc, retrieved_on, edited as integers), so
/// this hits the matching path of `rewrite_human_timestamps_bytes`.
fn lines_with_all_three_ts() -> &'static Vec<String> {
    static V: OnceLock<Vec<String>> = OnceLock::new();
    V.get_or_init(|| lines().iter().map(|s| s.to_string()).collect())
}

/// Variant of the fixture with all three timestamp keys stripped — exercises
/// the no-match fast-skip path in `rewrite_human_timestamps_bytes`.
fn lines_with_no_ts() -> &'static Vec<String> {
    static V: OnceLock<Vec<String>> = OnceLock::new();
    V.get_or_init(|| {
        lines()
            .iter()
            .map(|l| {
                let mut v: Value = serde_json::from_str(l).expect("fixture parses");
                if let Some(obj) = v.as_object_mut() {
                    obj.remove("created_utc");
                    obj.remove("retrieved_on");
                    obj.remove("edited");
                }
                serde_json::to_string(&v).expect("re-serialize")
            })
            .collect()
    })
}

/// Materialize a precomputed .zst by replicating the fixture lines. Built
/// once across the run; reused on subsequent invocations.
fn zst_fixture_path() -> &'static PathBuf {
    static PATH: OnceLock<PathBuf> = OnceLock::new();
    PATH.get_or_init(|| {
        let dir = std::env::temp_dir().join("retl-bench-fixtures");
        fs::create_dir_all(&dir).expect("create bench fixture dir");
        let path = dir.join(format!("inner_loops_sample_x{ZST_REPLICAS}.zst"));
        if !path.exists() {
            let f = fs::File::create(&path).expect("create zst fixture");
            let mut enc = zstd::stream::write::Encoder::new(f, 3).expect("zstd encoder");
            enc.include_checksum(true).expect("include_checksum");
            for _ in 0..ZST_REPLICAS {
                for line in lines() {
                    enc.write_all(line.as_bytes()).expect("zstd write line");
                    enc.write_all(b"\n").expect("zstd write newline");
                }
            }
            enc.finish().expect("zstd finalize");
        }
        path
    })
}

fn bench_for_each_line_cfg(c: &mut Criterion) {
    let zst_path = zst_fixture_path();
    let total_uncompressed_bytes: u64 = lines()
        .iter()
        .map(|l| l.len() as u64 + 1) // +1 for newline
        .sum::<u64>()
        * ZST_REPLICAS as u64;

    let mut group = c.benchmark_group("for_each_line_cfg");
    group.throughput(Throughput::Bytes(total_uncompressed_bytes));

    for &buf_bytes in &[16 * 1024usize, 64 * 1024, 256 * 1024] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("buf={}K", buf_bytes / 1024)),
            &buf_bytes,
            |b, &buf_bytes| {
                b.iter(|| {
                    let mut count: u64 = 0;
                    for_each_line_cfg(zst_path, buf_bytes, |_line| {
                        count += 1;
                        Ok(())
                    })
                    .expect("for_each_line_cfg");
                    black_box(count);
                });
            },
        );
    }
    group.finish();
}

fn bench_matches_minimal(c: &mut Criterion) {
    let recs = parsed_records();
    // Empty QuerySpec — exercises just the subreddit list-membership path,
    // which is what scales with target list size. `normalize()` is what the
    // caller would do in production (lowercases + sorts).
    let q: QuerySpec = QuerySpec::default().normalize();

    let mut group = c.benchmark_group("matches_minimal");
    group.throughput(Throughput::Elements(recs.len() as u64));

    for &n_targets in &[1usize, 10, 100] {
        // Build a target list whose first entry IS one of the fixture's
        // subreddits (so we measure both hit and miss work). Remaining entries
        // are synthetic but realistic-looking subreddit names.
        let mut targets: Vec<String> = Vec::with_capacity(n_targets);
        targets.push("programming".to_string());
        for i in 1..n_targets {
            targets.push(format!("filler_sub_{:04}", i));
        }
        // Mirror QuerySpec normalization (sorted/lower) so list_contains_ci
        // sees the same shape it would at runtime.
        for s in targets.iter_mut() {
            *s = s.to_lowercase();
        }
        targets.sort();
        targets.dedup();

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("targets={}", n_targets)),
            &targets,
            |b, targets| {
                b.iter(|| {
                    let mut hits: u32 = 0;
                    for r in recs.iter() {
                        if matches_minimal(r, Some(targets), &q) {
                            hits += 1;
                        }
                    }
                    black_box(hits);
                });
            },
        );
    }
    group.finish();
}

fn bench_rewrite_human_timestamps_bytes(c: &mut Criterion) {
    let with = lines_with_all_three_ts();
    let none = lines_with_no_ts();

    let mut group = c.benchmark_group("rewrite_human_timestamps_bytes");

    let with_bytes: u64 = with.iter().map(|s| s.len() as u64).sum();
    group.throughput(Throughput::Bytes(with_bytes));
    group.bench_function("all_three_keys_match", |b| {
        let mut buf = String::with_capacity(4 * 1024);
        b.iter(|| {
            for line in with.iter() {
                rewrite_human_timestamps_bytes(line, &mut buf);
                black_box(buf.as_str());
            }
        });
    });

    let none_bytes: u64 = none.iter().map(|s| s.len() as u64).sum();
    group.throughput(Throughput::Bytes(none_bytes));
    group.bench_function("none_present_skip", |b| {
        let mut buf = String::with_capacity(4 * 1024);
        b.iter(|| {
            for line in none.iter() {
                rewrite_human_timestamps_bytes(line, &mut buf);
                black_box(buf.as_str());
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_for_each_line_cfg,
    bench_matches_minimal,
    bench_rewrite_human_timestamps_bytes
);
criterion_main!(benches);
