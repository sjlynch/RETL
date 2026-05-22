use ahash::RandomState;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::thread::sleep;
use std::time::{Duration, Instant};

#[cfg(feature = "test-utils")]
use std::sync::{
    atomic::{AtomicUsize, Ordering as AtomicOrdering},
    Arc,
};

use crate::config::clamp_shard_count;
use crate::dedupe::BYTES_PER_MB;
use crate::key_extractor::KeyExtractor;
use crate::mem::{available_memory_fraction, is_low_memory};
use crate::ndjson::{read_line_capped, DEFAULT_MAX_LINE_BYTES};
use crate::util::smoothstep_memory_fraction;
use crate::zstd_jsonl::malformed_json_error;

use super::cfg::BucketingCfg;
use super::hash::{micro_state, stable_index};

/// Producer-side state for [`process_bucket_streaming`]: owns the
/// in-memory per-micro-bucket maps, their byte accounting, and a running
/// total. The three nested closures in the previous implementation
/// (`send_group`, `flush_bucket`, `flush_largest`) threaded three `&mut`
/// references through every call; folding them into a struct removes that
/// borrow plumbing.
struct MicroBucketState {
    maps: Vec<HashMap<String, Vec<String>>>,
    mb_bytes: Vec<usize>,
    total_bytes: usize,
}

impl MicroBucketState {
    fn with_capacity(n: usize) -> Self {
        let n = n.max(1);
        Self {
            maps: (0..n).map(|_| HashMap::new()).collect(),
            mb_bytes: vec![0; n],
            total_bytes: 0,
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.maps.len()
    }

    #[inline]
    fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    /// Append `line` to `maps[idx]` under `key`, charging `line.len() + 1`
    /// against the per-bucket and total byte counters.
    #[inline]
    fn insert(&mut self, idx: usize, key: String, line: String) {
        let add = line.len() + 1;
        self.maps[idx].entry(key).or_default().push(line);
        self.mb_bytes[idx] += add;
        self.total_bytes += add;
    }

    /// Drain micro-bucket `idx` and ship every `(key, group)` over `tx`.
    /// Returns `Err(())` if the consumer dropped the receiver — the producer
    /// breaks and lets the join surface the underlying error.
    ///
    /// Byte accounting is discharged **per group, after its send succeeds** —
    /// never up front. The swapped-out map stays fully resident until each
    /// `(k, v)` has been handed to the bounded channel, so subtracting its
    /// bytes before the send loop would make `total_bytes()` under-count live
    /// memory by up to a full flush during a backpressure stall (exactly when
    /// the producer's `target_bytes`/`is_low_memory` check needs it accurate).
    fn flush_bucket(
        &mut self,
        idx: usize,
        tx: &crossbeam_channel::Sender<(String, Vec<String>)>,
        #[cfg(feature = "test-utils")] buffered_bytes_metric: Option<&Arc<AtomicUsize>>,
    ) -> std::result::Result<(), ()> {
        if self.maps[idx].is_empty() {
            return Ok(());
        }
        let mut m = HashMap::new();
        std::mem::swap(&mut m, &mut self.maps[idx]);
        for (k, v) in m.into_iter() {
            // Byte charge for this group, mirroring `insert`'s `line.len() + 1`.
            let used: usize = v.iter().map(|line| line.len() + 1).sum();
            tx.send((k, v)).map_err(|_| ())?;
            // The group has left our maps for the channel; only now is it safe
            // to discharge its bytes.
            self.mb_bytes[idx] = self.mb_bytes[idx].saturating_sub(used);
            self.total_bytes = self.total_bytes.saturating_sub(used);
            #[cfg(feature = "test-utils")]
            if let Some(metric) = buffered_bytes_metric {
                metric.store(self.total_bytes, AtomicOrdering::Relaxed);
            }
        }
        Ok(())
    }

    /// Find the micro-bucket holding the most bytes and flush it via
    /// [`Self::flush_bucket`]. No-op when every bucket is empty.
    fn flush_largest(
        &mut self,
        tx: &crossbeam_channel::Sender<(String, Vec<String>)>,
        #[cfg(feature = "test-utils")] buffered_bytes_metric: Option<&Arc<AtomicUsize>>,
    ) -> std::result::Result<(), ()> {
        if self.maps.is_empty() {
            return Ok(());
        }
        let mut max_idx = 0usize;
        let mut max_val = 0usize;
        for (i, b) in self.mb_bytes.iter().enumerate() {
            if *b > max_val {
                max_val = *b;
                max_idx = i;
            }
        }
        if max_val > 0 {
            self.flush_bucket(
                max_idx,
                tx,
                #[cfg(feature = "test-utils")]
                buffered_bytes_metric,
            )?;
        }
        Ok(())
    }
}

/// Producer half of [`process_bucket_streaming`]: read NDJSON lines from
/// `r`, route each into a micro-bucket via `rs`/`key`, and hand the
/// largest map over to the consumer through `tx` whenever the adaptive
/// target or the soft-low-memory threshold is crossed.
///
/// Final loop flushes every remaining micro-bucket so the consumer sees a
/// complete drain before `tx` is dropped.
fn run_micro_bucket_producer(
    mut r: BufReader<File>,
    bucket: &Path,
    state: &mut MicroBucketState,
    rs: &RandomState,
    cfg: &BucketingCfg,
    key: &KeyExtractor,
    per_flush_cap: usize,
    tx: &crossbeam_channel::Sender<(String, Vec<String>)>,
    #[cfg(feature = "test-utils")] buffered_bytes_metric: Option<&Arc<AtomicUsize>>,
) -> Result<()> {
    let mb_count = state.len();

    // Adaptive target selection.
    let mut last_eval = Instant::now();
    let mut target_bytes: usize = cfg.micro_min_buf_mb * BYTES_PER_MB;

    // Reusable line buffer — avoids the per-line String allocation
    // from `BufReader::lines()` and the per-line
    // `serde_json::from_str::<Value>` DOM parse.
    let mut line = String::with_capacity(16 * 1024);
    let mut line_number: u64 = 0;
    loop {
        let n = read_line_capped(&mut r, &mut line, DEFAULT_MAX_LINE_BYTES, bucket).with_context(
            || {
                format!(
                    "read bucket {} near line {}",
                    bucket.display(),
                    line_number + 1
                )
            },
        )?;
        if n == 0 {
            break;
        }
        line_number += 1;
        if line.is_empty() {
            continue;
        }

        let k = match key
            .key_from_line(&line)
            .map_err(|e| malformed_json_error(bucket, line_number, e))?
        {
            Some(x) => x,
            None => continue,
        };
        let idx = stable_index(rs, &k, mb_count);

        state.insert(idx, k, line.clone());

        #[cfg(feature = "test-utils")]
        if let Some(m) = buffered_bytes_metric {
            m.store(state.total_bytes(), AtomicOrdering::Relaxed);
        }

        if last_eval.elapsed() >= Duration::from_millis(cfg.mem.adapt_cooldown_ms) {
            let scale = smoothstep_memory_fraction(
                available_memory_fraction(),
                cfg.mem.soft_low_frac,
                cfg.mem.high_frac,
            );
            let adaptive = ((cfg.micro_min_buf_mb as f64
                + (cfg.micro_max_buf_mb as f64 - cfg.micro_min_buf_mb as f64) * scale)
                .round() as usize)
                * BYTES_PER_MB;
            // Cap adaptive target by per_flush_cap so the bounded
            // channel — not the RAM-fraction sampler — is the
            // primary backpressure mechanism.
            target_bytes = adaptive.min(per_flush_cap);
            last_eval = Instant::now();
        }

        if state.total_bytes() >= target_bytes || is_low_memory(cfg.mem.soft_low_frac) {
            tracing::debug!(
                target = "retl::backpressure",
                stage = "bucketing.process_bucket_streaming",
                buffered_bytes = state.total_bytes(),
                target_bytes,
                "handing largest micro-bucket to on_group (bounded channel; producer blocks if full)"
            );
            if state
                .flush_largest(
                    tx,
                    #[cfg(feature = "test-utils")]
                    buffered_bytes_metric,
                )
                .is_err()
            {
                // consumer dropped rx (errored); break and let join surface it
                break;
            }
            #[cfg(feature = "test-utils")]
            if let Some(m) = buffered_bytes_metric {
                m.store(state.total_bytes(), AtomicOrdering::Relaxed);
            }
            if is_low_memory(cfg.hard_low_frac) {
                sleep(Duration::from_millis(cfg.backoff_ms));
            }
        }
    }

    // Final flush of any remaining micro-buckets.
    for i in 0..mb_count {
        if state
            .flush_bucket(
                i,
                tx,
                #[cfg(feature = "test-utils")]
                buffered_bytes_metric,
            )
            .is_err()
        {
            break;
        }
    }
    Ok(())
}

/// Stage 3: **in-memory** micro-bucket streaming with adaptive memory.
/// Reads `bucket_XXXX.jsonl`, routes lines to `micro_buckets` in memory using a
/// deterministic hasher, and **flushes groups adaptively** to the callback.
///
/// The callback receives `(key, Vec<String>)` for each group. When RAM is tight,
/// a given key may be flushed **multiple times** (partial groups); design your
/// downstream to merge if needed (our Stage 6 merge does this).
///
/// Producer/consumer split: line reading and per-bucket assembly run on the
/// calling thread; `on_group` runs on a worker thread that drains a bounded
/// `crossbeam_channel` (capacity `cfg.inflight_groups`). Sends block when
/// the consumer falls behind, so the producer stalls instead of growing
/// in-memory hash maps to multi-GiB peaks.
///
/// `key` must be consistent with Stage 1/2 to preserve routing consistency.
/// Lines are read into a reusable `String` buffer and the routing key is
/// extracted via [`KeyExtractor::key_from_line`] — no per-line
/// `serde_json::Value` allocation.
///
/// The `buffered_bytes_metric` parameter is a test-only hook gated behind
/// `feature="test-utils"` — when `Some`, the producer writes the current
/// in-memory buffered byte count to the supplied atomic after every line and
/// after every flush, so tests can verify the bounded channel caps memory.
/// Production builds (no `test-utils`) do not see this argument at all and
/// carry zero overhead.
pub fn process_bucket_streaming<F>(
    bucket: &Path,
    micro_buckets: usize,
    cfg: &BucketingCfg,
    mut on_group: F,
    key: &KeyExtractor,
    #[cfg(feature = "test-utils")] buffered_bytes_metric: Option<Arc<AtomicUsize>>,
) -> Result<()>
where
    F: FnMut(&str, Vec<String>) -> Result<()> + Send,
{
    // Open the input bucket; skip gracefully if missing.
    let file = match crate::util::open_with_default_backoff(bucket) {
        Ok(f) => f,
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                eprintln!("Warning: bucket missing, skipping: {}", bucket.display());
                return Ok(());
            }
            return Err(e).with_context(|| format!("open {}", bucket.display()));
        }
    };
    let r = BufReader::new(file);

    let rs = micro_state();

    // Clamp the fan-out like every other shard count in the crate
    // (`clamp_shard_count`: [1, MAX_SHARDS] + a one-shot warn). Without this a
    // caller of this pub re-exported API could pass a huge `micro_buckets` and
    // allocate that many HashMaps unbounded and unwarned.
    let micro_buckets = clamp_shard_count(micro_buckets, "bucketing::process_bucket_streaming");
    let mut state = MicroBucketState::with_capacity(micro_buckets);

    // Per-flush byte cap: with channel cap = inflight_groups, total inflight
    // bytes are bounded by (1 + inflight_groups) * per_flush_cap (one growing
    // producer-side bucket plus up to inflight_groups groups buffered in the
    // bounded channel) — ~1.125 GiB at the defaults, not 256 MiB. See CLAUDE.md
    // and the `ETLOptions::inflight_bytes` / `inflight_worst_case_peak_bytes`
    // docs.
    let per_flush_cap = if cfg.inflight_bytes > 0 {
        (cfg.inflight_bytes / 2).max(BYTES_PER_MB)
    } else {
        usize::MAX
    };

    let chan_cap = cfg.inflight_groups.max(1);
    let (tx, rx) = crossbeam_channel::bounded::<(String, Vec<String>)>(chan_cap);

    std::thread::scope(|s| -> Result<()> {
        let consumer_handle = s.spawn(move || -> Result<()> {
            while let Ok((k, v)) = rx.recv() {
                on_group(&k, v)?;
            }
            Ok(())
        });

        let producer_result = run_micro_bucket_producer(
            r,
            bucket,
            &mut state,
            &rs,
            cfg,
            key,
            per_flush_cap,
            &tx,
            #[cfg(feature = "test-utils")]
            buffered_bytes_metric.as_ref(),
        );

        // Always close tx so the consumer drains and exits.
        drop(tx);
        let consumer_result = consumer_handle.join().expect("on_group consumer panicked");
        // Surface consumer errors first (they have the actual user-callback failure).
        consumer_result?;
        producer_result?;
        Ok(())
    })
}
