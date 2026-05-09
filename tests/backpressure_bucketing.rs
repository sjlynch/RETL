//! Regression test for the bounded-channel backpressure inside
//! `process_bucket_streaming`.
//!
//! Without backpressure, a slow `on_group` consumer would let the producer
//! run ahead unbounded — peak memory would scale with input size rather than
//! with `cfg.inflight_bytes`. This test pins the contract:
//!
//!   * input  : ~50 MiB of synthetic NDJSON in a tempdir
//!   * config : cfg.inflight_bytes = 4 MiB, cfg.inflight_groups = 2
//!   * conszr : sleeps ~50 ms before returning from `on_group`
//!   * invar. : producer-side buffered_bytes stays under a documented cap
//!
//! Documented cap (BUFFERED_BYTES_CAP below): the producer-side total of all
//! per-microbucket maps must remain bounded at ~`per_flush_cap` plus one
//! line's overshoot, where `per_flush_cap = cfg.inflight_bytes / 2`. The
//! channel can hold up to `inflight_groups` already-handed-off groups in
//! addition, but those are not counted in `total_bytes`. We assert the cap
//! conservatively at `4 * per_flush_cap` (= `2 * cfg.inflight_bytes` =
//! 8 MiB) so the test is robust against scheduling jitter while still being
//! tight enough to fail loudly if the bound is removed (the unconstrained
//! case grows to ~50 MiB+).
//!
//! Requires the `test-utils` cargo feature so the `buffered_bytes_metric`
//! parameter is compiled into `process_bucket_streaming`. See `Cargo.toml`.

use retl::{process_bucket_streaming, BucketingCfg, KeyExtractor};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Synthesize ~`target_bytes` of NDJSON into `path`. Each record is a small
/// Reddit-shaped object (~200 B) with rotating authors. Author cardinality is
/// kept small (`AUTHOR_CARDINALITY`) on purpose: the consumer's per-group
/// sleep is multiplied by the number of distinct keys flushed, so a high
/// cardinality balloons the test runtime without strengthening the assertion
/// (the bound we test is on producer-side bytes, not on group count).
const AUTHOR_CARDINALITY: u64 = 16;
fn write_synthetic_bucket(path: &std::path::Path, target_bytes: usize) -> usize {
    let f = File::create(path).expect("create bucket file");
    let mut w = BufWriter::with_capacity(1 << 20, f);
    let mut written = 0usize;
    let mut i: u64 = 0;
    while written < target_bytes {
        // Rotate across a small fixed set of authors so multiple microbuckets
        // accumulate in parallel without exploding the consumer's group count.
        let author = format!("user_{:05}", i % AUTHOR_CARDINALITY);
        let line = format!(
            "{{\"author\":\"{}\",\"subreddit\":\"programming\",\"id\":\"r{:010}\",\"body\":\"backpressure regression payload {:08}\"}}",
            author, i, i
        );
        w.write_all(line.as_bytes()).expect("write line");
        w.write_all(b"\n").expect("write newline");
        written += line.len() + 1;
        i += 1;
    }
    w.flush().expect("flush writer");
    written
}

#[test]
fn process_bucket_streaming_blocks_producer_when_consumer_is_slow() {
    let dir = tempfile::tempdir().expect("tempdir");
    let bucket = dir.path().join("bucket_0000.jsonl");

    // Synthesize ~50 MiB of NDJSON.
    let target_bytes = 50 * 1024 * 1024;
    let actual = write_synthetic_bucket(&bucket, target_bytes);
    assert!(
        actual >= target_bytes,
        "synthetic bucket too small: {} < {}",
        actual,
        target_bytes
    );

    // Tight backpressure budget: 4 MiB inflight, 2 in-flight groups.
    // per_flush_cap = inflight_bytes / 2 = 2 MiB.
    let cfg = BucketingCfg {
        soft_low_frac: 0.0,    // never engage the soft-low (free-RAM) path
        hard_low_frac: 0.0,    // never sleep on hard-low
        high_frac: 1.0,
        backoff_ms: 0,
        micro_min_buf_mb: 64,  // generous adaptive target — we want the
        micro_max_buf_mb: 64,  //   per_flush_cap to be the binding constraint
        adapt_cooldown_ms: 50,
        inflight_bytes: 4 * 1024 * 1024,
        inflight_groups: 2,
    };

    // Cap derivation:
    //   per_flush_cap = inflight_bytes / 2 = 2 MiB.
    //   total_bytes (the metric) sums producer-side maps only. Once it
    //   crosses target_bytes (which is min(adaptive, per_flush_cap) = 2 MiB),
    //   the largest microbucket is flushed. Worst-case overshoot before the
    //   next flush is bounded by the size of one fat map slot.
    //   We assert 4 * per_flush_cap to absorb scheduling jitter but still
    //   fail an unconstrained regression (which would reach ~50 MiB).
    const PER_FLUSH_CAP: usize = 2 * 1024 * 1024;
    const BUFFERED_BYTES_CAP: usize = 4 * PER_FLUSH_CAP; // 8 MiB

    let metric = Arc::new(AtomicUsize::new(0));
    let peak_observer = Arc::new(AtomicUsize::new(0));

    // Spawn a watcher that polls the metric and tracks the max value seen.
    // This is more reliable than checking only inside on_group, since the
    // producer's peak occurs between flushes (i.e. while the consumer is
    // still asleep).
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let watcher = {
        let metric = Arc::clone(&metric);
        let peak = Arc::clone(&peak_observer);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let cur = metric.load(Ordering::Relaxed);
                let mut prev = peak.load(Ordering::Relaxed);
                while cur > prev {
                    match peak.compare_exchange_weak(
                        prev,
                        cur,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => break,
                        Err(now) => prev = now,
                    }
                }
                std::thread::sleep(Duration::from_millis(2));
            }
        })
    };

    let key = KeyExtractor::author_lowercase_fast();
    let group_count = std::sync::atomic::AtomicUsize::new(0);

    process_bucket_streaming(
        &bucket,
        4,
        &cfg,
        |_k, v| {
            // Slow consumer: ~50 ms per group. With ~50 MiB input and
            // tight inflight budget, this WILL stall the producer if
            // backpressure works; it WILL grow memory unbounded if not.
            std::thread::sleep(Duration::from_millis(50));
            group_count.fetch_add(1, Ordering::Relaxed);
            // Black-box: read the group so the optimizer can't skip it.
            assert!(!v.is_empty(), "consumer received empty group");
            Ok(())
        },
        &key,
        Some(Arc::clone(&metric)),
    )
    .expect("process_bucket_streaming");

    stop.store(true, Ordering::Relaxed);
    watcher.join().expect("watcher join");

    let peak = peak_observer.load(Ordering::Relaxed);
    let groups = group_count.load(Ordering::Relaxed);

    assert!(
        groups > 0,
        "consumer never received any groups (test setup is wrong)"
    );
    assert!(
        peak > 0,
        "buffered_bytes metric was never updated — was the test-utils gate \
         compiled in? peak={}",
        peak
    );

    // The contract: the bounded channel keeps producer-side memory
    // bounded by ~per_flush_cap, NOT by input size.
    assert!(
        peak <= BUFFERED_BYTES_CAP,
        "peak buffered_bytes exceeded documented cap: peak={} bytes (cap={} bytes); \
         backpressure is not bounding producer memory. groups={}",
        peak,
        BUFFERED_BYTES_CAP,
        groups
    );

    // Sanity: the peak should be near per_flush_cap, not a tiny fraction —
    // otherwise the test isn't actually exercising the flush threshold.
    assert!(
        peak >= PER_FLUSH_CAP / 4,
        "peak buffered_bytes={} is suspiciously small (per_flush_cap={}); \
         the synthetic input may be too tiny to reach the flush threshold",
        peak,
        PER_FLUSH_CAP
    );
}
