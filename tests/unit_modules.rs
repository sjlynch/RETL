//! Direct unit tests for `bucketing`, `dedupe`, `kv_shard`, and `ndjson` modules.
//!
//! Today's other tests drive these only indirectly through the ETL pipeline with
//! a tiny corpus, which never exercises the adaptive flush branches in
//! `process_bucket_streaming` / `build_runs_sorted`. Here we drive them with
//! ~10K records and force tight memory targets so the streaming/flush paths
//! actually fire.

#[path = "common/mod.rs"]
mod common;

use common::*;
use retl::{
    build_runs_sorted, bucketize_shards, merge_runs_sorted, partition_stage1,
    process_bucket_streaming, BucketingCfg, DedupeCfg, KeyExtractor, NdjsonReader,
    NdjsonWriter, ShardedKVWriter,
};

use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;

// ---------- ndjson ----------

#[test]
fn ndjson_writer_then_reader_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("rt.ndjson");

    let mut w = NdjsonWriter::create(&p, 8 * 1024).unwrap();
    let inputs = ["alpha", "beta", "gamma with spaces", "δelta"];
    for s in inputs.iter() {
        w.write_line(s).unwrap();
    }
    w.finish().unwrap();

    let mut r = NdjsonReader::open(&p, 8 * 1024).unwrap();
    let mut got: Vec<String> = Vec::new();
    let mut buf = String::new();
    loop {
        let n = r.read_line(&mut buf).unwrap();
        if n == 0 {
            break;
        }
        got.push(buf.clone());
    }
    assert_eq!(got, inputs.iter().map(|s| s.to_string()).collect::<Vec<_>>());
}

#[test]
fn ndjson_reader_strips_crlf_and_handles_blank_lines() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("crlf.ndjson");

    // Write a mix of \n and \r\n endings, plus a blank line.
    let mut f = File::create(&p).unwrap();
    f.write_all(b"one\r\ntwo\n\r\nthree\r\n").unwrap();
    drop(f);

    let mut r = NdjsonReader::open(&p, 8 * 1024).unwrap();
    let mut buf = String::new();
    let mut got = Vec::new();
    loop {
        let n = r.read_line(&mut buf).unwrap();
        if n == 0 {
            break;
        }
        got.push(buf.clone());
    }
    assert_eq!(got, vec!["one", "two", "", "three"]);
}

#[test]
fn ndjson_writer_finish_atomic_promotes_temp() {
    let dir = tempfile::tempdir().unwrap();
    let tmp = dir.path().join("out.ndjson.inprogress");
    let final_path = dir.path().join("out.ndjson");

    let mut w = NdjsonWriter::create(&tmp, 8 * 1024).unwrap();
    w.write_line("a").unwrap();
    w.write_line("b").unwrap();
    w.finish_atomic(&final_path).unwrap();

    assert!(!tmp.exists(), "temp should be removed after atomic promote");
    assert!(final_path.exists(), "final path must exist");
    let lines: Vec<String> = BufReader::new(File::open(&final_path).unwrap())
        .lines()
        .filter_map(|l| l.ok())
        .collect();
    assert_eq!(lines, vec!["a", "b"]);
}

// ---------- bucketing ----------

/// Helper: write `lines` as plain NDJSON (uncompressed) to a path. Bucketing
/// expects plain NDJSON inputs (its callers decode .zst -> .jsonl upstream).
fn write_ndjson(path: &std::path::Path, lines: &[String]) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut f = File::create(path).unwrap();
    for l in lines {
        f.write_all(l.as_bytes()).unwrap();
        f.write_all(b"\n").unwrap();
    }
}

fn count_lines(path: &std::path::Path) -> usize {
    BufReader::new(File::open(path).unwrap())
        .lines()
        .filter(|l| matches!(l, Ok(s) if !s.is_empty()))
        .count()
}

#[test]
fn bucketing_partition_stage1_routes_all_records_and_is_deterministic() {
    let dir = tempfile::tempdir().unwrap();

    // Build ~3K records with ~750 distinct authors.
    let mut lines: Vec<String> = Vec::with_capacity(3000);
    for i in 0..3000 {
        let author = format!("user_{:04}", i % 750);
        lines.push(
            serde_json::json!({
                "author": author,
                "subreddit":"programming",
                "id": format!("rc{:06}", i),
                "body":"x"
            })
            .to_string(),
        );
    }
    let in_path = dir.path().join("in.ndjson");
    write_ndjson(&in_path, &lines);

    // Run partition twice; bytes/line counts and per-shard totals must match exactly.
    fn run(in_path: &std::path::Path, out: &std::path::Path) -> Vec<PathBuf> {
        let key = KeyExtractor::author_lowercase_fast();
        partition_stage1(
            std::slice::from_ref(&in_path.to_path_buf()),
            out,
            8,
            &key,
        )
        .unwrap()
    }
    let a = run(&in_path, &dir.path().join("a"));
    let b = run(&in_path, &dir.path().join("b"));
    assert_eq!(a.len(), 8);
    assert_eq!(b.len(), 8);

    // Total written == input
    let total_a: usize = a.iter().map(|p| count_lines(p)).sum();
    let total_b: usize = b.iter().map(|p| count_lines(p)).sum();
    assert_eq!(total_a, 3000);
    assert_eq!(total_b, 3000);

    // Determinism: every key lands in the same shard across runs.
    let mut author_to_shard: HashMap<String, usize> = HashMap::new();
    for (i, p) in a.iter().enumerate() {
        for line in BufReader::new(File::open(p).unwrap()).lines().flatten() {
            let v: Value = serde_json::from_str(&line).unwrap();
            let a = v["author"].as_str().unwrap().to_lowercase();
            if let Some(prev) = author_to_shard.insert(a.clone(), i) {
                assert_eq!(prev, i, "author {} split across shards within one run", a);
            }
        }
    }
    for (i, p) in b.iter().enumerate() {
        for line in BufReader::new(File::open(p).unwrap()).lines().flatten() {
            let v: Value = serde_json::from_str(&line).unwrap();
            let a = v["author"].as_str().unwrap().to_lowercase();
            assert_eq!(
                author_to_shard.get(&a).copied(),
                Some(i),
                "author {} routed to a different shard between runs",
                a
            );
        }
    }
}

#[test]
fn bucketing_process_bucket_streaming_drives_adaptive_flush_with_10k_records() {
    let dir = tempfile::tempdir().unwrap();

    // ~10K records to force the flush branch when target_bytes is tiny.
    let mut lines: Vec<String> = Vec::with_capacity(10_000);
    for i in 0..10_000 {
        let author = format!("user_{:05}", i % 1000);
        lines.push(
            serde_json::json!({
                "author": author,
                "subreddit":"programming",
                "id": format!("rc{:08}", i),
                "body":"some body"
            })
            .to_string(),
        );
    }
    let bucket = dir.path().join("bucket_0000.jsonl");
    write_ndjson(&bucket, &lines);

    // Force adaptive flushes by setting a very tight buffer target.
    // micro_min_buf_mb=0 and micro_max_buf_mb=0 means ~0-byte target — flush every line group.
    let cfg = BucketingCfg {
        soft_low_frac: 0.999, // pretend we're "low" so the soft-low branch triggers too
        hard_low_frac: 0.0,   // never trigger the hard backoff sleep
        high_frac: 1.0,
        backoff_ms: 0,
        micro_min_buf_mb: 0,
        micro_max_buf_mb: 0,
        adapt_cooldown_ms: 1, // re-evaluate fast
        inflight_bytes: 0,    // disable cap; let tight buffer drive flushes
        inflight_groups: 8,
    };

    let key = KeyExtractor::author_lowercase_fast();
    let mut groups_seen: usize = 0;
    let mut totals: BTreeMap<String, usize> = BTreeMap::new();
    process_bucket_streaming(
        &bucket,
        4,
        &cfg,
        |k, v| {
            groups_seen += 1;
            *totals.entry(k.to_string()).or_insert(0) += v.len();
            Ok(())
        },
        &key,
        // The `test-utils` feature adds a buffered-bytes metric arg used by
        // tests/backpressure_bucketing.rs. This test doesn't observe that
        // metric, but must still pass `None` when the feature is enabled.
        #[cfg(feature = "test-utils")]
        None,
    )
    .unwrap();

    // 1000 distinct keys; with adaptive flushes a key may be flushed multiple times,
    // so groups_seen >= 1000 — and in this aggressive config, strictly greater.
    assert!(
        groups_seen >= 1000,
        "expected at least 1000 group flushes; got {}",
        groups_seen
    );
    assert!(
        groups_seen > 1000,
        "tight buffer should force >1 flush per key (sanity for adaptive branch); got {}",
        groups_seen
    );

    // No matter how many partial flushes happened, totals must match input counts.
    assert_eq!(totals.len(), 1000);
    let sum: usize = totals.values().sum();
    assert_eq!(sum, 10_000);
    // Each key should have ~10 records (10000 / 1000), exactly 10 by our construction.
    for v in totals.values() {
        assert_eq!(*v, 10);
    }
}

#[test]
fn bucketing_bucketize_shard_splits_into_n_buckets() {
    let dir = tempfile::tempdir().unwrap();

    let mut lines: Vec<String> = Vec::with_capacity(2000);
    for i in 0..2000 {
        let author = format!("u{:04}", i % 200);
        lines.push(
            serde_json::json!({"author": author, "id": format!("x{}", i)}).to_string(),
        );
    }
    let shard = dir.path().join("stage1_0000.jsonl");
    write_ndjson(&shard, &lines);

    let key = KeyExtractor::author_lowercase_fast();
    let buckets = bucketize_shards(
        std::slice::from_ref(&shard),
        &dir.path().join("buckets"),
        4,
        &key,
    )
    .unwrap();

    assert_eq!(buckets.len(), 4);
    let total: usize = buckets.iter().map(|p| count_lines(p)).sum();
    assert_eq!(total, 2000);
}

// ---------- dedupe ----------

#[test]
fn dedupe_build_and_merge_groups_by_key_in_sorted_order() {
    let dir = tempfile::tempdir().unwrap();

    // Three duplicate entries per author across ~600 authors == 1800 records.
    let in_path = dir.path().join("dedupe_in.ndjson");
    let mut lines = Vec::with_capacity(1800);
    for i in 0..1800 {
        let author = format!("user_{:04}", i % 600);
        lines.push(
            serde_json::json!({"author": author, "id": format!("c{}", i)}).to_string(),
        );
    }
    write_ndjson(&in_path, &lines);

    // Tight memory target -> multiple runs -> merge has real work.
    let cfg = DedupeCfg {
        min_buf_mb: 0,
        max_buf_mb: 0,
        soft_low_frac: 0.999,
        high_frac: 1.0,
        adapt_cooldown_ms: 1,
        read_buf_bytes: 8 * 1024,
        write_buf_bytes: 8 * 1024,
        inflight_bytes: 0, // disable cap so test's tight target_bytes drives flushes
    };
    let runs_dir = dir.path().join("runs");
    let key = KeyExtractor::author_lowercase_fast();
    let runs = build_runs_sorted(&in_path, &runs_dir, &key, &cfg).unwrap();

    // With buf_mb=0 and 1800 records, we expect many runs (>= 2 to make merge meaningful).
    assert!(runs.len() >= 2, "expected multiple runs, got {}", runs.len());

    // Each run's keys must be sorted within the run.
    for run in &runs {
        let mut seen_keys: Vec<String> = Vec::new();
        for line in BufReader::new(File::open(run).unwrap()).lines().flatten() {
            let v: Value = serde_json::from_str(&line).unwrap();
            let k = v["author"].as_str().unwrap().to_lowercase();
            if seen_keys.last() != Some(&k) {
                seen_keys.push(k);
            }
        }
        let mut sorted = seen_keys.clone();
        sorted.sort();
        assert_eq!(seen_keys, sorted, "keys not sorted within run {}", run.display());
    }

    // Merge: the callback receives all lines for one key as a group; we emit a
    // single output line per key. Verify keys appear once and globally sorted.
    let out = dir.path().join("merged.ndjson");
    merge_runs_sorted(&runs, &out, &key, &cfg, |k, group, w| {
        // Each key has exactly 3 records (1800 / 600 distinct = 3).
        assert_eq!(group.len(), 3, "expected 3 dupes per key for {}", k);
        // Emit only the key as a single line (we're testing the merge plumbing).
        w.write_all(k.as_bytes())?;
        w.write_all(b"\n")?;
        Ok(())
    })
    .unwrap();

    let merged: Vec<String> = BufReader::new(File::open(&out).unwrap())
        .lines()
        .filter_map(|l| l.ok())
        .filter(|s| !s.is_empty())
        .collect();
    assert_eq!(merged.len(), 600, "one output line per distinct key");
    let mut sorted = merged.clone();
    sorted.sort();
    assert_eq!(merged, sorted, "merge output must be globally key-sorted");
    // No duplicates in output keys.
    let unique: BTreeSet<&String> = merged.iter().collect();
    assert_eq!(unique.len(), merged.len());
}

#[test]
fn dedupe_single_run_promotes_directly_to_output() {
    let dir = tempfile::tempdir().unwrap();
    let in_path = dir.path().join("small.ndjson");
    let mut lines = Vec::with_capacity(20);
    for i in 0..20 {
        let author = format!("u{:02}", i % 5);
        lines.push(
            serde_json::json!({"author": author, "id": format!("c{}", i)}).to_string(),
        );
    }
    write_ndjson(&in_path, &lines);

    // Generous buffer -> exactly one run; merge_runs_sorted promotes it directly.
    let cfg = DedupeCfg {
        min_buf_mb: 64,
        max_buf_mb: 64,
        soft_low_frac: 0.0,
        high_frac: 1.0,
        adapt_cooldown_ms: 10_000,
        read_buf_bytes: 8 * 1024,
        write_buf_bytes: 8 * 1024,
        inflight_bytes: 0, // disable cap; the test wants a single big run
    };
    let runs_dir = dir.path().join("runs");
    let key = KeyExtractor::author_lowercase_fast();
    let runs = build_runs_sorted(&in_path, &runs_dir, &key, &cfg).unwrap();
    assert_eq!(runs.len(), 1);

    let out = dir.path().join("merged.ndjson");
    merge_runs_sorted(&runs, &out, &key, &cfg, |_k, _g, _w| Ok(())).unwrap();
    assert!(out.exists());
}

// ---------- kv_shard ----------

#[test]
fn kv_shard_sum_reduces_per_key_totals() {
    let dir = tempfile::tempdir().unwrap();
    let kv = ShardedKVWriter::create(dir.path(), "sumtest", 4).unwrap();

    // 600 distinct keys, each written 5 times with values 1..=5 -> sum == 15 per key.
    for round in 1..=5i64 {
        for i in 0..600 {
            kv.write_kv(&format!("k_{:04}", i), round).unwrap();
        }
    }
    let shards = kv.reduce_sum("sumtest").unwrap();
    assert_eq!(shards.len(), 4);

    let mut totals: HashMap<String, i64> = HashMap::new();
    for p in shards {
        for line in BufReader::new(File::open(&p).unwrap()).lines().flatten() {
            let (k, v) = line.split_once('\t').unwrap();
            totals.insert(k.to_string(), v.parse().unwrap());
        }
    }
    assert_eq!(totals.len(), 600);
    for v in totals.values() {
        assert_eq!(*v, 15);
    }
}

#[test]
fn kv_shard_min_picks_smallest_value_per_key() {
    let dir = tempfile::tempdir().unwrap();
    let kv = ShardedKVWriter::create(dir.path(), "mintest", 2).unwrap();

    // For each key, the minimum value is `i` (we also write i+10, i+20).
    for i in 0..50i64 {
        let k = format!("k_{:03}", i);
        kv.write_kv(&k, i + 20).unwrap();
        kv.write_kv(&k, i + 10).unwrap();
        kv.write_kv(&k, i).unwrap();
    }
    let shards = kv.reduce_min("mintest").unwrap();
    let mut totals: HashMap<String, i64> = HashMap::new();
    for p in shards {
        for line in BufReader::new(File::open(&p).unwrap()).lines().flatten() {
            let (k, v) = line.split_once('\t').unwrap();
            totals.insert(k.to_string(), v.parse().unwrap());
        }
    }
    assert_eq!(totals.len(), 50);
    for (k, v) in &totals {
        let i: i64 = k.trim_start_matches("k_").parse().unwrap();
        assert_eq!(*v, i, "min for {} should be {}, got {}", k, i, v);
    }
}
