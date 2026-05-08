//! Regression bench for the parallel shard-merge in `src/aggregate.rs`.
//!
//! Marked `#[ignore]` so it does not run on plain `cargo test` (timing on a
//! loaded CI runner is noisy and the goal is to detect regressions, not pin
//! a specific speedup). Run manually with:
//!
//! ```text
//! cargo test --test aggregate_merge_bench --release -- --ignored --nocapture
//! ```

use retl::{merge_aggregator_shards_parallel, merge_aggregator_shards_serial, Aggregator};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::Instant;

/// Tiny bench-only aggregator: a histogram over fixed-size string keys.
/// Picked over a single counter so deserialize + merge actually do work.
#[derive(Default, Serialize, Deserialize)]
struct Hist {
    counts: std::collections::HashMap<String, u64>,
}
impl Aggregator for Hist {
    fn ingest(&mut self, _record: &Value) {}
    fn merge(&mut self, other: Self) {
        for (k, v) in other.counts {
            *self.counts.entry(k).or_insert(0) += v;
        }
    }
}

fn write_shards(dir: &std::path::Path, n: usize, keys_per_shard: usize) -> Vec<PathBuf> {
    std::fs::create_dir_all(dir).unwrap();
    let mut shards = Vec::with_capacity(n);
    for i in 0..n {
        let mut h = Hist::default();
        for k in 0..keys_per_shard {
            // Mix shared and unique keys so merging actually combines entries.
            let key = if k % 4 == 0 {
                format!("shared_{}", k)
            } else {
                format!("shard{}_key{}", i, k)
            };
            *h.counts.entry(key).or_insert(0) += 1;
        }
        let p = dir.join(format!("agg_{:04}.json", i));
        let f = File::create(&p).unwrap();
        let mut w = BufWriter::new(f);
        serde_json::to_writer(&mut w, &h).unwrap();
        w.flush().unwrap();
        shards.push(p);
    }
    shards
}

#[test]
#[ignore = "timing-based regression bench; run with --ignored --nocapture"]
fn merge_parallel_beats_serial_on_many_shards() {
    let tmp = tempfile::tempdir().unwrap();
    // 256 shards × 2000 keys: small enough to count as "small shards" but
    // large enough that per-shard deserialize+merge dominates rayon's
    // dispatch overhead, so parallelism actually shows up.
    let shards = write_shards(tmp.path(), 256, 2000);

    // Warm the page cache so we measure CPU/dispatch, not first-read I/O.
    let _: Hist = merge_aggregator_shards_serial(&shards, None).unwrap();

    let t0 = Instant::now();
    let serial: Hist = merge_aggregator_shards_serial(&shards, None).unwrap();
    let serial_dt = t0.elapsed();

    let t1 = Instant::now();
    let parallel: Hist = merge_aggregator_shards_parallel(&shards, None).unwrap();
    let parallel_dt = t1.elapsed();

    // Correctness: both paths must produce identical totals.
    assert_eq!(serial.counts.len(), parallel.counts.len());
    for (k, v) in &serial.counts {
        assert_eq!(parallel.counts.get(k), Some(v), "mismatch on key {}", k);
    }

    let speedup = serial_dt.as_secs_f64() / parallel_dt.as_secs_f64();
    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    eprintln!(
        "merge bench: shards=256 cores={} serial={:?} parallel={:?} speedup={:.2}x",
        cores, serial_dt, parallel_dt, speedup
    );

    // Soft regression guard: parallel must not be dramatically *slower* than
    // serial. We deliberately don't assert >=2x — the task notes that strict
    // factors are too flaky for CI, and this test is `#[ignore]`d anyway.
    // 1.5x slower would mean the parallel path is broken.
    assert!(
        parallel_dt.as_secs_f64() <= serial_dt.as_secs_f64() * 1.5,
        "parallel merge regressed: serial={:?} parallel={:?}",
        serial_dt,
        parallel_dt
    );
}
