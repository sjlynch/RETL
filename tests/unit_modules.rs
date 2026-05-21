//! Direct unit tests for `bucketing`, `dedupe`, `kv_shard`, and `ndjson` modules.
//!
//! Today's other tests drive these only indirectly through the ETL pipeline with
//! a tiny corpus, which never exercises the adaptive flush branches in
//! `process_bucket_streaming` / `build_runs_sorted`. Here we drive them with
//! ~10K records and force tight memory targets so the streaming/flush paths
//! actually fire.

#[path = "common/mod.rs"]
mod common;

use retl::{
    build_runs_sorted, bucketize_shards, merge_runs_sorted, partition_stage1,
    partition_stage1_with_key_stats, process_bucket_streaming, BucketingCfg, DedupeCfg,
    KeyExtractor, NdjsonReader, NdjsonWriter, ShardedKVWriter,
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

/// Build `n` records over ~`n/4` distinct authors. The author keying scheme
/// matches what the bucketing/dedupe stress tests originally used at much
/// larger N — keeping the shape identical means a `#[ignore]`'d wide-N twin
/// run can exercise the same code path at 10×/30× scale without forking the
/// fixture builder.
fn bucketing_synthetic_records(n: usize) -> Vec<String> {
    let distinct = (n / 4).max(1);
    let mut lines = Vec::with_capacity(n);
    for i in 0..n {
        let author = format!("user_{:05}", i % distinct);
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
    lines
}

/// Drive `partition_stage1` twice over `n_records` synthetic NDJSON records
/// and assert:
///   - both runs produce `shards` shard files
///   - per-run totals equal `n_records`
///   - the (key → shard) mapping is identical across runs (key determinism)
///
/// Used by the daily-fast test (`n_records=200`) and the `#[ignore]`d
/// wide-N stress variant.
fn assert_partition_stage1_determinism(n_records: usize, shards: usize) {
    let dir = tempfile::tempdir().unwrap();
    let lines = bucketing_synthetic_records(n_records);
    let in_path = dir.path().join("in.ndjson");
    write_ndjson(&in_path, &lines);

    fn run(in_path: &std::path::Path, out: &std::path::Path, shards: usize) -> Vec<PathBuf> {
        let key = KeyExtractor::author_lowercase_fast();
        partition_stage1(std::slice::from_ref(&in_path.to_path_buf()), out, shards, &key).unwrap()
    }
    let a = run(&in_path, &dir.path().join("a"), shards);
    let b = run(&in_path, &dir.path().join("b"), shards);
    assert_eq!(a.len(), shards);
    assert_eq!(b.len(), shards);

    let total_a: usize = a.iter().map(|p| count_lines(p)).sum();
    let total_b: usize = b.iter().map(|p| count_lines(p)).sum();
    assert_eq!(total_a, n_records);
    assert_eq!(total_b, n_records);

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
fn bucketing_partition_stage1_routes_all_records_and_is_deterministic() {
    // Daily-fast scale: 200 records / ~50 distinct authors / 8 shards is
    // plenty to put multiple keys per shard and exercise the determinism
    // invariant on a non-trivial mapping. Wide-N coverage is the `#[ignore]`d
    // stress variant at the bottom of this file.
    assert_partition_stage1_determinism(200, 8);
}

/// Drive `process_bucket_streaming` with `n_records` synthetic records over
/// `n_keys` distinct keys (so each key has `n_records / n_keys` rows by
/// construction). Asserts:
///   - at least `n_keys` group flushes (each key seen at least once)
///   - strictly more than `n_keys` flushes (tight buffer forces partial flush
///     of the same key) — this is the adaptive-branch invariant
///   - total ingested rows equal `n_records`
///   - every key sees exactly `n_records / n_keys` records
///
/// The numerical invariants don't care about absolute scale — they just need
/// `n_records > n_keys` so flushes can happen mid-key. 1100/110 is plenty;
/// the original 10000/1000 was overkill. The `#[ignore]`d wide-N stress at
/// the bottom of this file exercises the original scale.
fn assert_process_bucket_streaming_adaptive(n_records: usize, n_keys: usize) {
    assert!(
        n_records % n_keys == 0,
        "test helper requires n_records divisible by n_keys"
    );
    let per_key = n_records / n_keys;

    let dir = tempfile::tempdir().unwrap();
    let mut lines: Vec<String> = Vec::with_capacity(n_records);
    for i in 0..n_records {
        let author = format!("user_{:05}", i % n_keys);
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

    // Force adaptive flushes by setting a ~0-byte target buffer.
    let cfg = BucketingCfg {
        mem: retl::AdaptiveMemCfg {
            soft_low_frac: 0.999, // pretend we're "low" so soft-low branch fires
            high_frac: 1.0,
            adapt_cooldown_ms: 1,
        },
        hard_low_frac: 0.0,
        backoff_ms: 0,
        micro_min_buf_mb: 0,
        micro_max_buf_mb: 0,
        inflight_bytes: 0,
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
        #[cfg(feature = "test-utils")]
        None,
    )
    .unwrap();

    assert!(
        groups_seen >= n_keys,
        "expected at least {n_keys} group flushes; got {groups_seen}"
    );
    assert!(
        groups_seen > n_keys,
        "tight buffer should force >1 flush per key (adaptive-branch sanity); got {groups_seen}"
    );

    assert_eq!(totals.len(), n_keys);
    let sum: usize = totals.values().sum();
    assert_eq!(sum, n_records);
    for v in totals.values() {
        assert_eq!(*v, per_key);
    }
}

#[test]
fn bucketing_process_bucket_streaming_drives_adaptive_flush() {
    // Daily-fast scale: 1100 records / 110 keys / 10 per key. The
    // adaptive-branch invariants don't strengthen with N — see the
    // `#[ignore]`d wide-N stress variant for the original 10K/1000/10 scale.
    assert_process_bucket_streaming_adaptive(1100, 110);
}

/// Drive `bucketize_shards` over `n_records` records spread across `n_buckets`
/// output partitions and assert: bucket count and total row count are exact.
/// Daily test uses 200 records; the wide-N stress variant uses 2000.
fn assert_bucketize_shard_splits(n_records: usize, n_buckets: usize) {
    let dir = tempfile::tempdir().unwrap();
    let distinct = (n_records / 10).max(1);
    let mut lines: Vec<String> = Vec::with_capacity(n_records);
    for i in 0..n_records {
        let author = format!("u{:04}", i % distinct);
        lines.push(serde_json::json!({"author": author, "id": format!("x{}", i)}).to_string());
    }
    let shard = dir.path().join("stage1_0000.jsonl");
    write_ndjson(&shard, &lines);

    let key = KeyExtractor::author_lowercase_fast();
    let buckets = bucketize_shards(
        std::slice::from_ref(&shard),
        &dir.path().join("buckets"),
        n_buckets,
        &key,
    )
    .unwrap();

    assert_eq!(buckets.len(), n_buckets);
    let total: usize = buckets.iter().map(|p| count_lines(p)).sum();
    assert_eq!(total, n_records);
}

#[test]
fn bucketing_bucketize_shard_splits_into_n_buckets() {
    assert_bucketize_shard_splits(200, 4);
}

#[test]
fn bucketing_partition_stage1_with_key_stats_counts_unkeyable_lines() {
    use std::sync::atomic::{AtomicU64, Ordering};

    let dir = tempfile::tempdir().unwrap();
    // 3 keyable records (have `author`) + 2 records the author key extractor
    // cannot key (no `author` field) — the latter must be dropped *and counted*.
    let lines = vec![
        serde_json::json!({"author": "alice", "id": "a1"}).to_string(),
        serde_json::json!({"author": "bob", "id": "b1"}).to_string(),
        serde_json::json!({"id": "noauthor1"}).to_string(),
        serde_json::json!({"author": "carol", "id": "c1"}).to_string(),
        serde_json::json!({"id": "noauthor2"}).to_string(),
    ];
    let in_path = dir.path().join("in.ndjson");
    write_ndjson(&in_path, &lines);

    let key = KeyExtractor::author_lowercase_fast();
    let failed = AtomicU64::new(0);
    let shards = partition_stage1_with_key_stats(
        std::slice::from_ref(&in_path.to_path_buf()),
        &dir.path().join("out"),
        4,
        &key,
        Some(&failed),
    )
    .unwrap();

    let total: usize = shards.iter().map(|p| count_lines(p)).sum();
    assert_eq!(total, 3, "only keyable lines are routed to shards");
    assert_eq!(
        failed.load(Ordering::Relaxed),
        2,
        "both unkeyable lines must be counted as key-extraction failures"
    );

    // The no-counter entry point still drops the same lines, just silently.
    let shards = partition_stage1(
        std::slice::from_ref(&in_path.to_path_buf()),
        &dir.path().join("out2"),
        4,
        &key,
    )
    .unwrap();
    let total: usize = shards.iter().map(|p| count_lines(p)).sum();
    assert_eq!(total, 3);
}

// ---------- dedupe ----------

/// Drive build_runs_sorted + merge_runs_sorted with `n_keys` distinct authors
/// each duplicated `dupes_per_key` times, asserting:
///   - tight memory target produces ≥ 2 runs (so the k-way merge actually runs)
///   - keys within each run are sorted (the in-run invariant)
///   - merge callback receives exactly `dupes_per_key` group entries per key
///   - merge output has one line per distinct key, globally sorted, unique
///
/// Daily-fast scale is 20 keys × 3 dupes = 60 records; wide-N stress at the
/// bottom of this file uses the original 600 × 3.
fn assert_dedupe_groups_by_key(n_keys: usize, dupes_per_key: usize) {
    let n_records = n_keys * dupes_per_key;
    let dir = tempfile::tempdir().unwrap();
    let in_path = dir.path().join("dedupe_in.ndjson");
    let mut lines = Vec::with_capacity(n_records);
    for i in 0..n_records {
        let author = format!("user_{:04}", i % n_keys);
        lines.push(serde_json::json!({"author": author, "id": format!("c{}", i)}).to_string());
    }
    write_ndjson(&in_path, &lines);

    let cfg = DedupeCfg {
        mem: retl::AdaptiveMemCfg {
            soft_low_frac: 0.999,
            high_frac: 1.0,
            adapt_cooldown_ms: 1,
        },
        min_buf_mb: 0,
        max_buf_mb: 0,
        read_buf_bytes: 8 * 1024,
        write_buf_bytes: 8 * 1024,
        inflight_bytes: 0,
    };
    let runs_dir = dir.path().join("runs");
    let key = KeyExtractor::author_lowercase_fast();
    let runs = build_runs_sorted(&in_path, &runs_dir, &key, &cfg).unwrap();

    assert!(runs.len() >= 2, "expected multiple runs, got {}", runs.len());

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
        assert_eq!(
            seen_keys,
            sorted,
            "keys not sorted within run {}",
            run.display()
        );
    }

    let out = dir.path().join("merged.ndjson");
    merge_runs_sorted(&runs, &out, &key, &cfg, |k, group, w| {
        assert_eq!(
            group.len(),
            dupes_per_key,
            "expected {dupes_per_key} dupes per key for {k}"
        );
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
    assert_eq!(merged.len(), n_keys, "one output line per distinct key");
    let mut sorted = merged.clone();
    sorted.sort();
    assert_eq!(merged, sorted, "merge output must be globally key-sorted");
    let unique: BTreeSet<&String> = merged.iter().collect();
    assert_eq!(unique.len(), merged.len());
}

#[test]
fn dedupe_build_and_merge_groups_by_key_in_sorted_order() {
    // Daily-fast scale: 20 keys × 3 dupes = 60 records. With buf_mb=0 even
    // this tiny input produces multiple runs, so the k-way merge invariants
    // still fire. Wide-N stress is the `#[ignore]`d variant below.
    assert_dedupe_groups_by_key(20, 3);
}

#[test]
fn dedupe_single_run_still_applies_merge_callback() {
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

    // Generous buffer -> exactly one run; merge_runs_sorted must still apply
    // the caller's merge callback so single-run inputs are deduped too.
    let cfg = DedupeCfg {
        mem: retl::AdaptiveMemCfg { soft_low_frac: 0.0, high_frac: 1.0, adapt_cooldown_ms: 10_000 },
        min_buf_mb: 64,
        max_buf_mb: 64,
        read_buf_bytes: 8 * 1024,
        write_buf_bytes: 8 * 1024,
        inflight_bytes: 0, // disable cap; the test wants a single big run
    };
    let runs_dir = dir.path().join("runs");
    let key = KeyExtractor::author_lowercase_fast();
    let runs = build_runs_sorted(&in_path, &runs_dir, &key, &cfg).unwrap();
    assert_eq!(runs.len(), 1);

    let out = dir.path().join("merged.ndjson");
    merge_runs_sorted(&runs, &out, &key, &cfg, |k, group, w| {
        assert_eq!(group.len(), 4, "20 rows over 5 authors -> 4 per key");
        writeln!(w, "{k}")?;
        Ok(())
    }).unwrap();

    let merged = common::read_lines(&out);
    assert_eq!(merged, vec!["u00", "u01", "u02", "u03", "u04"]);
}

/// Regression: before the atomic-write fix, `merge_runs_sorted` staged its
/// temp via `output.with_extension("ndjson.inprogress")`, which strips the
/// final extension. Two concurrent merges with destinations `out/x.txt` and
/// `out/x.json` in the same directory both resolved to `out/x.ndjson.inprogress`
/// and clobbered each other's staged file. The fix routes through
/// `<parent>/_staging/<basename>.retl-<pid>-<nonce>.inprogress`, so the basename
/// (including the original extension) is preserved and each writer gets a
/// unique PID/nonce-suffixed staged path.
#[test]
fn merge_runs_sorted_concurrent_writes_share_dir_with_distinct_stems() {
    use std::sync::{Arc, Barrier};
    use std::thread;

    let dir = tempfile::tempdir().unwrap();
    let out_dir = dir.path().to_path_buf();

    // Two independent input run files, one per "merge job". Each contains
    // one author's worth of (sorted-by-key) lines so `merge_runs_sorted` has
    // real work to do — empty-runs would short-circuit through a different
    // code path.
    let runs_dir_a = out_dir.join("runs_a");
    let runs_dir_b = out_dir.join("runs_b");
    fs::create_dir_all(&runs_dir_a).unwrap();
    fs::create_dir_all(&runs_dir_b).unwrap();
    let run_a = runs_dir_a.join("run_0001.ndjson");
    let run_b = runs_dir_b.join("run_0001.ndjson");
    fs::write(
        &run_a,
        b"{\"author\":\"alice\"}\n{\"author\":\"alice\"}\n{\"author\":\"bob\"}\n",
    )
    .unwrap();
    fs::write(
        &run_b,
        b"{\"author\":\"carol\"}\n{\"author\":\"carol\"}\n{\"author\":\"dave\"}\n",
    )
    .unwrap();

    // Destinations share a stem (`x.*`) and parent directory — the legacy
    // `with_extension("ndjson.inprogress")` would map both to the same temp.
    let dest_txt = out_dir.join("x.txt");
    let dest_json = out_dir.join("x.json");

    let cfg = DedupeCfg {
        mem: retl::AdaptiveMemCfg { soft_low_frac: 0.0, high_frac: 1.0, adapt_cooldown_ms: 10_000 },
        min_buf_mb: 1,
        max_buf_mb: 1,
        read_buf_bytes: 8 * 1024,
        write_buf_bytes: 8 * 1024,
        inflight_bytes: 0,
    };

    // Force both merges to be active inside the per-group callback at the
    // same instant — that's exactly the window where the old code would
    // have raced on the shared `out/x.ndjson.inprogress` temp.
    let barrier = Arc::new(Barrier::new(2));

    let cfg_a = cfg.clone();
    let dest_a = dest_txt.clone();
    let runs_a = vec![run_a.clone()];
    let bar_a = Arc::clone(&barrier);
    let h_a = thread::spawn(move || {
        let key = KeyExtractor::author_lowercase_fast();
        let mut first = true;
        merge_runs_sorted(&runs_a, &dest_a, &key, &cfg_a, |k, _group, w| {
            if first {
                bar_a.wait();
                first = false;
            }
            writeln!(w, "{}=A", k)?;
            Ok(())
        })
    });

    let cfg_b = cfg.clone();
    let dest_b = dest_json.clone();
    let runs_b = vec![run_b.clone()];
    let bar_b = Arc::clone(&barrier);
    let h_b = thread::spawn(move || {
        let key = KeyExtractor::author_lowercase_fast();
        let mut first = true;
        merge_runs_sorted(&runs_b, &dest_b, &key, &cfg_b, |k, _group, w| {
            if first {
                bar_b.wait();
                first = false;
            }
            writeln!(w, "{}=B", k)?;
            Ok(())
        })
    });

    h_a.join().unwrap().expect("merge A succeeded");
    h_b.join().unwrap().expect("merge B succeeded");

    // Both destinations exist and contain their own merge output — neither
    // truncated the other's staged file.
    assert!(dest_txt.is_file(), "destination x.txt was not published");
    assert!(dest_json.is_file(), "destination x.json was not published");

    let txt = common::read_lines(&dest_txt);
    let json = common::read_lines(&dest_json);
    assert_eq!(txt, vec!["alice=A", "bob=A"], "x.txt mismatched");
    assert_eq!(json, vec!["carol=B", "dave=B"], "x.json mismatched");

    // Staged files were cleaned up by the atomic-write helper after publish.
    let staging = out_dir.join("_staging");
    if staging.exists() {
        let leftovers: Vec<PathBuf> = fs::read_dir(&staging)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|s| s.to_str())
                    .map(|n| n.ends_with(".inprogress"))
                    .unwrap_or(false)
            })
            .collect();
        assert!(
            leftovers.is_empty(),
            "successful merges should leave no staged leftovers, found: {:?}",
            leftovers
        );
    }
}

/// A failed `merge_runs_sorted` must still reclaim its `run_*` scratch files.
/// The old code deleted them only after the atomic publish succeeded, so a
/// merge error (here: a callback that bails) left `run_*` files behind — and a
/// dedupe re-run with a reused `runs_dir` could pick up stale runs.
#[test]
fn merge_runs_sorted_failure_removes_run_scratch() {
    let dir = tempfile::tempdir().unwrap();
    let in_path = dir.path().join("dedupe_in.ndjson");
    let mut lines = Vec::with_capacity(60);
    for i in 0..60 {
        let author = format!("user_{:04}", i % 20);
        lines.push(serde_json::json!({"author": author, "id": format!("c{}", i)}).to_string());
    }
    write_ndjson(&in_path, &lines);

    // buf_mb = 0 forces multiple runs so there is real merge scratch to clean.
    let cfg = DedupeCfg {
        mem: retl::AdaptiveMemCfg { soft_low_frac: 0.999, high_frac: 1.0, adapt_cooldown_ms: 1 },
        min_buf_mb: 0,
        max_buf_mb: 0,
        read_buf_bytes: 8 * 1024,
        write_buf_bytes: 8 * 1024,
        inflight_bytes: 0,
    };
    let runs_dir = dir.path().join("runs");
    let key = KeyExtractor::author_lowercase_fast();
    let runs = build_runs_sorted(&in_path, &runs_dir, &key, &cfg).unwrap();
    assert!(!runs.is_empty(), "expected at least one run file");
    for r in &runs {
        assert!(r.is_file(), "run file missing before merge: {}", r.display());
    }

    let out = dir.path().join("merged.ndjson");
    let result = merge_runs_sorted(&runs, &out, &key, &cfg, |_k, _group, _w| {
        Err(anyhow::anyhow!("merge callback deliberately fails"))
    });
    assert!(result.is_err(), "merge should fail when the callback errors");

    for r in &runs {
        assert!(
            !r.exists(),
            "failed merge leaked run scratch file: {}",
            r.display()
        );
    }
}

// ---------- kv_shard ----------

/// Drive ShardedKVWriter.reduce_sum with `n_keys` distinct keys, each written
/// `rounds` times with values 1..=rounds. Per-key sum must equal the triangle
/// number `rounds*(rounds+1)/2`. Daily-fast scale is 60×5; wide-N stress
/// uses 600×5.
fn assert_kv_shard_sum(n_keys: usize, rounds: i64, n_buckets: usize) {
    let expected_sum: i64 = rounds * (rounds + 1) / 2;
    let dir = tempfile::tempdir().unwrap();
    let kv = ShardedKVWriter::create(dir.path(), "sumtest", n_buckets).unwrap();
    for round in 1..=rounds {
        for i in 0..n_keys {
            kv.write_kv(&format!("k_{:04}", i), round).unwrap();
        }
    }
    let shards = kv.reduce_sum("sumtest").unwrap();
    assert_eq!(shards.len(), n_buckets);
    let mut totals: HashMap<String, i64> = HashMap::new();
    for p in shards {
        for line in BufReader::new(File::open(&p).unwrap()).lines().flatten() {
            let (k, v) = line.split_once('\t').unwrap();
            totals.insert(k.to_string(), v.parse().unwrap());
        }
    }
    assert_eq!(totals.len(), n_keys);
    for v in totals.values() {
        assert_eq!(*v, expected_sum);
    }
}

#[test]
fn kv_shard_sum_reduces_per_key_totals() {
    // 60 keys × 5 rounds is plenty; wide-N stress uses 600 × 5.
    assert_kv_shard_sum(60, 5, 4);
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

// ---------------------------------------------------------------------------
// Wide-N stress variants (`#[ignore]`d for daily iteration).
//
// The four bucketing/dedupe/kv tests above each run a `assert_*` helper at a
// daily-fast scale (~60-1100 records). The same invariants don't strengthen
// at large N — but the original 3K/10K/2K/1800/600×5 scales did sometimes
// catch perf regressions or memory-buffering pathologies that smaller inputs
// missed. This single `#[ignore]`d test re-exercises every helper at the
// original full scale; run before merging perf-sensitive changes with:
//
//     cargo test --test unit_modules -- --ignored --nocapture
// ---------------------------------------------------------------------------

#[test]
#[ignore = "wide-N stress; run with --ignored before perf-sensitive merges"]
fn stress_bucketing_dedupe_kv_at_original_scale() {
    assert_partition_stage1_determinism(3000, 8);
    assert_process_bucket_streaming_adaptive(10_000, 1000);
    assert_bucketize_shard_splits(2000, 4);
    assert_dedupe_groups_by_key(600, 3);
    assert_kv_shard_sum(600, 5, 4);
}
