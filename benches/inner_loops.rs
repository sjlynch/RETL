//! Criterion microbenchmarks for the three hot inner-loop primitives that the
//! recent T6 ahash + drop-per-line-to_lowercase work touched. Defends future
//! perf changes against silent regressions.
//!
//! Run via:
//!   cargo bench --bench inner_loops -- --save-baseline pre   (before change)
//!   cargo bench --bench inner_loops -- --baseline pre        (after change)

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use retl::{
    bucketize_shards, for_each_line_cfg, matches_minimal, parse_minimal, partition_stage1,
    process_bucket_streaming, rewrite_human_timestamps_bytes, BucketingCfg, KeyExtractor,
    MinimalRecord, QuerySpec, WhitelistTokenizer,
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

/// Compare the two-pass `tokenize_into → rewrite_human_timestamps_bytes`
/// composition against the fused single-pass
/// `tokenize_and_rewrite_timestamps_into`. The fused method exists to avoid
/// the second walk over already-projected bytes; the criterion harness here
/// gates that win against future regressions.
fn bench_whitelist_with_timestamps(c: &mut Criterion) {
    let lines = lines_with_all_three_ts();
    // A realistic Reddit-export whitelist: a handful of small fields that
    // include the three timestamp keys.
    let fields = [
        "id",
        "author",
        "subreddit",
        "body",
        "score",
        "created_utc",
        "retrieved_on",
        "edited",
    ];
    let tok = WhitelistTokenizer::new(fields);

    let total_bytes: u64 = lines.iter().map(|s| s.len() as u64).sum();

    let mut group = c.benchmark_group("whitelist_with_timestamps");
    group.throughput(Throughput::Bytes(total_bytes));

    group.bench_function("two_pass", |b| {
        let mut tok_buf = String::with_capacity(4 * 1024);
        let mut ts_buf = String::with_capacity(4 * 1024);
        b.iter(|| {
            for line in lines.iter() {
                tok.tokenize_into(line, &mut tok_buf)
                    .expect("tokenize_into");
                rewrite_human_timestamps_bytes(&tok_buf, &mut ts_buf);
                black_box(ts_buf.as_str());
            }
        });
    });

    group.bench_function("fused", |b| {
        let mut buf = String::with_capacity(4 * 1024);
        b.iter(|| {
            for line in lines.iter() {
                tok.tokenize_and_rewrite_timestamps_into(line, &mut buf)
                    .expect("tokenize_and_rewrite_timestamps_into");
                black_box(buf.as_str());
            }
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// `bucketing` group
//
// Defends the perf change that dropped per-line `serde_json::Value` parses in
// `partition_stage1`, `bucketize_shards`, and `process_bucket_streaming` in
// favor of `KeyExtractor::key_from_line` (which uses the `MinimalRecord` fast
// path), and that parallelized stage 2 across input shards via rayon.
//
// Each bench:
//   - materializes a plain NDJSON fixture once into a temp dir (replicated
//     fixture lines, ~tens of MB) so the inner-loop work dominates per-iter
//     overhead.
//   - runs the full stage on that fixture inside `b.iter`.
//
// Compare baselines:
//   cargo bench --bench inner_loops -- --save-baseline pre  (before change)
//   cargo bench --bench inner_loops -- --baseline pre       (after change)
//
// Perf bound: the new code path elides ~1-3 KiB of per-line allocations per
// record (the `serde_json::Value` DOM), so we expect ≥2x throughput
// improvement on these benches.
// ---------------------------------------------------------------------------

/// Replication factor for the bucketing fixtures. Bigger than ZST_REPLICAS
/// since these benches run plain NDJSON (no decompression overhead) so we
/// need more rows for the per-line work to dominate.
const BUCKETING_REPLICAS: usize = 1500;

/// Materialize a plain NDJSON fixture (fixture lines replicated `BUCKETING_REPLICAS`
/// times) at a stable temp path. Built once per process; reused by every bucketing
/// bench. Returns `(path, total_bytes)`.
fn ndjson_fixture() -> &'static (PathBuf, u64) {
    static F: OnceLock<(PathBuf, u64)> = OnceLock::new();
    F.get_or_init(|| {
        let dir = std::env::temp_dir().join("retl-bench-fixtures");
        fs::create_dir_all(&dir).expect("create bench fixture dir");
        let path = dir.join(format!("inner_loops_bucketing_x{BUCKETING_REPLICAS}.jsonl"));
        let bytes_per_pass: u64 = lines().iter().map(|l| l.len() as u64 + 1).sum();
        let total = bytes_per_pass * BUCKETING_REPLICAS as u64;
        if !path.exists() {
            let f = fs::File::create(&path).expect("create ndjson fixture");
            let mut w = std::io::BufWriter::with_capacity(1 << 20, f);
            for _ in 0..BUCKETING_REPLICAS {
                for line in lines() {
                    w.write_all(line.as_bytes()).expect("write line");
                    w.write_all(b"\n").expect("write newline");
                }
            }
            w.flush().expect("flush ndjson fixture");
        }
        (path, total)
    })
}

fn bench_partition_stage1(c: &mut Criterion) {
    let (in_path, total_bytes) = ndjson_fixture();
    let inputs = vec![in_path.clone()];

    let mut group = c.benchmark_group("bucketing/partition_stage1");
    group.throughput(Throughput::Bytes(*total_bytes));
    group.sample_size(10);

    let key = KeyExtractor::author_lowercase_fast();
    group.bench_function("author_lower_fast/8shards", |b| {
        b.iter_with_setup(
            || {
                let dir = tempfile::Builder::new()
                    .prefix("retl-bench-stage1-")
                    .tempdir()
                    .expect("tempdir");
                dir
            },
            |dir| {
                let out = dir.path();
                let paths = partition_stage1(&inputs, out, 8, &key)
                    .expect("partition_stage1");
                black_box(paths);
            },
        );
    });

    group.finish();
}

fn bench_bucketize_shards(c: &mut Criterion) {
    let (in_path, total_bytes) = ndjson_fixture();

    // Materialize stage1 shards once, outside the timed region.
    static SHARDS: OnceLock<(tempfile::TempDir, Vec<PathBuf>)> = OnceLock::new();
    let (_keep, shards) = SHARDS.get_or_init(|| {
        let dir = tempfile::Builder::new()
            .prefix("retl-bench-stage1-fixture-")
            .tempdir()
            .expect("tempdir");
        let key = KeyExtractor::author_lowercase_fast();
        let paths = partition_stage1(&[in_path.clone()], dir.path(), 8, &key)
            .expect("partition_stage1 fixture");
        (dir, paths)
    });

    let mut group = c.benchmark_group("bucketing/bucketize_shards");
    group.throughput(Throughput::Bytes(*total_bytes));
    group.sample_size(10);

    let key = KeyExtractor::author_lowercase_fast();
    group.bench_function("author_lower_fast/4buckets", |b| {
        b.iter_with_setup(
            || {
                tempfile::Builder::new()
                    .prefix("retl-bench-stage2-")
                    .tempdir()
                    .expect("tempdir")
            },
            |dir| {
                let out = dir.path();
                let paths = bucketize_shards(shards, out, 4, &key)
                    .expect("bucketize_shards");
                black_box(paths);
            },
        );
    });

    group.finish();
}

fn bench_process_bucket_streaming(c: &mut Criterion) {
    let (in_path, total_bytes) = ndjson_fixture();

    let mut group = c.benchmark_group("bucketing/process_bucket_streaming");
    group.throughput(Throughput::Bytes(*total_bytes));
    group.sample_size(10);

    // Generous in-memory budget so flush/backpressure overhead doesn't
    // dominate; this bench is about the per-line read+key-extract loop.
    let cfg = BucketingCfg {
        mem: retl::AdaptiveMemCfg {
            soft_low_frac: 0.0,
            high_frac: 1.0,
            adapt_cooldown_ms: 1_000,
        },
        hard_low_frac: 0.0,
        backoff_ms: 0,
        micro_min_buf_mb: 256,
        micro_max_buf_mb: 256,
        inflight_bytes: 0,
        inflight_groups: 8,
    };
    let key = KeyExtractor::author_lowercase_fast();

    group.bench_function("author_lower_fast/4microbuckets", |b| {
        b.iter(|| {
            let mut groups: u64 = 0;
            process_bucket_streaming(
                in_path,
                4,
                &cfg,
                |_k, v| {
                    groups += v.len() as u64;
                    Ok(())
                },
                &key,
                // Match the cfg-gated `buffered_bytes_metric` parameter when
                // benches are run under `--features test-utils`.
                #[cfg(feature = "test-utils")]
                None,
            )
            .expect("process_bucket_streaming");
            black_box(groups);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_for_each_line_cfg,
    bench_matches_minimal,
    bench_rewrite_human_timestamps_bytes,
    bench_whitelist_with_timestamps,
    bench_partition_stage1,
    bench_bucketize_shards,
    bench_process_bucket_streaming,
);
criterion_main!(benches);
