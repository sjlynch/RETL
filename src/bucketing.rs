use anyhow::{Context, Result};
use ahash::RandomState;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hash, Hasher};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::thread::sleep;
use std::time::{Duration, Instant};

#[cfg(feature = "test-utils")]
use std::sync::{atomic::{AtomicUsize, Ordering as AtomicOrdering}, Arc};

use crate::config::ETLOptions;
use crate::key_extractor::KeyExtractor;
use crate::mem::{available_memory_fraction, is_low_memory, AdaptiveMemCfg};
use crate::util::{
    create_dir_all_with_backoff, create_with_backoff, open_with_backoff,
    smoothstep_memory_fraction,
};
use crate::zstd_jsonl::malformed_json_error;

/// Adaptive streaming configuration used during micro-bucket processing.
#[derive(Clone, Debug)]
pub struct BucketingCfg {
    /// Shared adaptive-memory policy (soft_low_frac, high_frac, adapt_cooldown_ms).
    pub mem: AdaptiveMemCfg,
    pub hard_low_frac: f64,       // when below, yield briefly
    pub backoff_ms: u64,          // sleep when under hard threshold
    pub micro_min_buf_mb: usize,  // min target buffering when RAM is tight
    pub micro_max_buf_mb: usize,  // max target buffering when RAM is plentiful
    /// Hard cap on bytes inflight between the line-reader producer and the
    /// `on_group` consumer. Caps the per-bucket flush target so the bounded
    /// channel — not the RAM-fraction sampler — is the primary backpressure
    /// mechanism. 0 disables the cap.
    pub inflight_bytes: usize,
    /// Bounded channel capacity between producer and on_group consumer.
    /// Sized for ~8 in-flight per-key groups so producers stall when the
    /// consumer falls behind.
    pub inflight_groups: usize,
}

impl Default for BucketingCfg {
    fn default() -> Self {
        Self {
            mem: AdaptiveMemCfg::default(),
            hard_low_frac: 0.10,
            backoff_ms: 25,
            micro_min_buf_mb: 128,
            micro_max_buf_mb: 4096,
            // Default 256 MiB inflight budget; with channel cap 8 the per-flush
            // bucket target is capped at ~32 MiB.
            inflight_bytes: 256 * 1024 * 1024,
            inflight_groups: 8,
        }
    }
}

impl From<&ETLOptions> for BucketingCfg {
    fn from(opts: &ETLOptions) -> Self {
        Self {
            mem: opts.adaptive_mem.clone(),
            inflight_bytes: opts.inflight_bytes,
            inflight_groups: opts.inflight_groups,
            ..Self::default()
        }
    }
}

#[inline]
fn stable_index(state: &RandomState, key: &str, parts: usize) -> usize {
    let mut h = state.build_hasher();
    key.hash(&mut h);
    (h.finish() as usize) % parts.max(1)
}

/// Shared shard-router used by both Stage 1 (`partition_stage1`) and
/// Stage 2 (`bucketize_shards`). Creates `shards.max(1)` mutex-guarded
/// `BufWriter<File>`s named `{file_prefix}_{:04}.jsonl` under `out_dir`,
/// then `par_iter`s over `inputs`, hashing each line's routing key with
/// `state` + [`stable_index`] and appending the line to the indexed writer.
/// All writers are flushed before return.
///
/// `state` is the per-stage `RandomState` — Stage 1 and Stage 2 must use
/// different seeds, otherwise re-bucketing in Stage 2 is a no-op.
fn route_lines_to_shards(
    inputs: &[PathBuf],
    out_dir: &Path,
    shards: usize,
    state: RandomState,
    file_prefix: &str,
    key: &KeyExtractor,
) -> Result<Vec<PathBuf>> {
    use parking_lot::Mutex;
    use rayon::prelude::*;
    use std::fs::File;
    use std::io::{BufReader, BufWriter};

    create_dir_all_with_backoff(out_dir, 16, 50)
        .with_context(|| format!("create shard output dir {}", out_dir.display()))?;

    let shard_count = shards.max(1);
    let mut writers: Vec<Mutex<BufWriter<File>>> = Vec::with_capacity(shard_count);
    let mut paths: Vec<PathBuf> = Vec::with_capacity(shard_count);
    for i in 0..shard_count {
        let p = out_dir.join(format!("{}_{:04}.jsonl", file_prefix, i));
        let f = create_with_backoff(&p, 16, 50)
            .with_context(|| format!("create {}", p.display()))?;
        writers.push(Mutex::new(BufWriter::new(f)));
        paths.push(p);
    }

    inputs.par_iter().try_for_each(|p| -> Result<()> {
        let f = open_with_backoff(p, 16, 50).with_context(|| format!("open {}", p.display()))?;
        let mut r = BufReader::new(f);
        // Reusable line buffer — avoids the per-line String allocation from
        // `BufReader::lines()` (mirrors `zstd_jsonl::for_each_line_attempt`).
        let mut line = String::with_capacity(16 * 1024);
        let mut line_number: u64 = 0;
        loop {
            line.clear();
            let n = r.read_line(&mut line)?;
            if n == 0 { break; }
            line_number += 1;
            // Strip trailing \r?\n so the slice fed to KeyExtractor matches
            // what extractors see in the rest of the pipeline.
            if line.ends_with('\n') {
                line.pop();
                if line.ends_with('\r') { line.pop(); }
            }
            if line.is_empty() { continue; }
            if let Some(k) = key
                .key_from_line(&line)
                .map_err(|e| malformed_json_error(p, line_number, e))?
            {
                let idx = stable_index(&state, &k, shards);
                let mut w = writers[idx].lock();
                w.write_all(line.as_bytes())?;
                w.write_all(b"\n")?;
            }
        }
        Ok(())
    })?;
    for w in &writers { w.lock().flush()?; }
    Ok(paths)
}

/// Stage 1: shard arbitrary NDJSON inputs by `key` into `shards` files.
/// Output files are named: `stage1_XXXX.jsonl`.
///
/// `key` decides which routing key to use (e.g. author). Avoids the
/// per-line `serde_json::Value` DOM parse by going through
/// [`KeyExtractor::key_from_line`], which uses the `MinimalRecord` fast path
/// for the common Reddit fields.
pub fn partition_stage1(
    inputs: &[PathBuf],
    out_dir: &Path,
    shards: usize,
    key: &KeyExtractor,
) -> Result<Vec<PathBuf>> {
    // Deterministic sharding state for Stage 1. Distinct from Stage 2's
    // seeds so a key landing in shard `i` here will redistribute on Stage 2.
    let rs = RandomState::with_seeds(
        0x1111_2222_3333_4444,
        0x5555_6666_7777_8888,
        0x9999_aaaa_bbbb_cccc,
        0xdddd_eeee_ffff_1234,
    );
    route_lines_to_shards(inputs, out_dir, shards, rs, "stage1", key)
}

/// Stage 2: re-bucket Stage 1 shards into `buckets` files by the **same key**.
/// Output files are named: `bucket_XXXX.jsonl`.
///
/// Parallelizes across the input shards via rayon — Stage 1 was parallelized
/// previously; Stage 2 now matches it. Drops the per-line
/// `serde_json::Value` parse (uses [`KeyExtractor::key_from_line`]) and reads
/// lines into a reusable `String` buffer.
pub fn bucketize_shards(
    shards: &[PathBuf],
    out_dir: &Path,
    buckets: usize,
    key: &KeyExtractor,
) -> Result<Vec<PathBuf>> {
    // Deterministic sharding for Stage 2. Seeds intentionally differ from
    // Stage 1 so re-bucketing actually redistributes keys across the
    // bucket files; do not unify these.
    let rs = RandomState::with_seeds(
        0xabcdef01_abcdef02,
        0xabcdef03_abcdef04,
        0xabcdef05_abcdef06,
        0xabcdef07_abcdef08,
    );
    route_lines_to_shards(shards, out_dir, buckets, rs, "bucket", key)
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
/// `cfg(any(test, feature="test-utils"))` — when `Some`, the producer writes
/// the current in-memory buffered byte count to the supplied atomic after
/// every line and after every flush, so tests can verify the bounded channel
/// caps memory. Production builds (no `test-utils`) do not see this argument
/// at all and carry zero overhead.
pub fn process_bucket_streaming<F>(
    bucket: &Path,
    micro_buckets: usize,
    cfg: &BucketingCfg,
    mut on_group: F,
    key: &KeyExtractor,
    #[cfg(feature = "test-utils")]
    buffered_bytes_metric: Option<Arc<AtomicUsize>>,
) -> Result<()>
where
    F: FnMut(&str, Vec<String>) -> Result<()> + Send,
{
    // Open the input bucket; skip gracefully if missing.
    let file = match open_with_backoff(bucket, 16, 50) {
        Ok(f) => f,
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                eprintln!("Warning: bucket missing, skipping: {}", bucket.display());
                return Ok(());
            }
            return Err(e).with_context(|| format!("open {}", bucket.display()));
        }
    };
    let mut r = BufReader::new(file);

    // Deterministic sharding for micro-buckets (in-memory).
    let rs = RandomState::with_seeds(
        0x0a0b_0c0d_0e0f_a1a2,
        0xb1b2_b3b4_b5b6_c1c2,
        0xd1d2_d3d4_d5d6_e1e2,
        0xf1f2_f3f4_f5f6_0102,
    );

    let mb_count = micro_buckets.max(1);
    let mut maps: Vec<HashMap<String, Vec<String>>> = (0..mb_count).map(|_| HashMap::new()).collect();
    let mut mb_bytes: Vec<usize> = vec![0; mb_count];
    let mut total_bytes: usize = 0;

    // Adaptive target selection.
    let mut last_eval = Instant::now();
    let mut target_bytes: usize = cfg.micro_min_buf_mb * 1024 * 1024;

    // Per-flush byte cap: with channel cap = inflight_groups, total inflight
    // bytes are bounded by 2 * per_flush_cap (one growing producer-side bucket
    // + the channel + the consumer's current group).
    let per_flush_cap = if cfg.inflight_bytes > 0 {
        (cfg.inflight_bytes / 2).max(1024 * 1024)
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

        // Helper: flush one micro-bucket (by index).
        let send_group = |k: String, v: Vec<String>| -> std::result::Result<(), ()> {
            // tx.send blocks here when consumer falls behind → backpressure
            tx.send((k, v)).map_err(|_| ())
        };

        let flush_bucket = |idx: usize,
                                maps: &mut [HashMap<String, Vec<String>>],
                                mb_bytes: &mut [usize],
                                total_bytes: &mut usize| -> std::result::Result<(), ()> {
            if maps[idx].is_empty() { return Ok(()); }
            let mut m = HashMap::new();
            std::mem::swap(&mut m, &mut maps[idx]);
            let used = mb_bytes[idx];
            mb_bytes[idx] = 0;
            if *total_bytes >= used { *total_bytes -= used; } else { *total_bytes = 0; }
            for (k, v) in m.into_iter() {
                send_group(k, v)?;
            }
            Ok(())
        };

        let flush_largest = |maps: &mut [HashMap<String, Vec<String>>],
                                 mb_bytes: &mut [usize],
                                 total_bytes: &mut usize| -> std::result::Result<(), ()> {
            if maps.is_empty() { return Ok(()); }
            let mut max_idx = 0usize;
            let mut max_val = 0usize;
            for (i, b) in mb_bytes.iter().enumerate() {
                if *b > max_val { max_val = *b; max_idx = i; }
            }
            if max_val > 0 {
                flush_bucket(max_idx, maps, mb_bytes, total_bytes)?;
            }
            Ok(())
        };

        let producer_result: Result<()> = (|| -> Result<()> {
            // Reusable line buffer — avoids the per-line String allocation
            // from `BufReader::lines()` and the per-line
            // `serde_json::from_str::<Value>` DOM parse.
            let mut line = String::with_capacity(16 * 1024);
            let mut line_number: u64 = 0;
            loop {
                line.clear();
                let n = r.read_line(&mut line)?;
                if n == 0 { break; }
                line_number += 1;
                if line.ends_with('\n') {
                    line.pop();
                    if line.ends_with('\r') { line.pop(); }
                }
                if line.is_empty() { continue; }

                let k = match key
                    .key_from_line(&line)
                    .map_err(|e| malformed_json_error(bucket, line_number, e))?
                {
                    Some(x) => x,
                    None => continue,
                };
                let idx = stable_index(&rs, &k, mb_count);

                let add = line.len() + 1;
                let entry = maps[idx].entry(k).or_default();
                entry.push(line.clone());

                mb_bytes[idx] += add;
                total_bytes += add;

                #[cfg(feature = "test-utils")]
                if let Some(m) = buffered_bytes_metric.as_ref() {
                    m.store(total_bytes, AtomicOrdering::Relaxed);
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
                        * 1024
                        * 1024;
                    // Cap adaptive target by per_flush_cap so the bounded
                    // channel — not the RAM-fraction sampler — is the
                    // primary backpressure mechanism.
                    target_bytes = adaptive.min(per_flush_cap);
                    last_eval = Instant::now();
                }

                if total_bytes >= target_bytes || is_low_memory(cfg.mem.soft_low_frac) {
                    tracing::debug!(
                        target = "retl::backpressure",
                        stage = "bucketing.process_bucket_streaming",
                        buffered_bytes = total_bytes,
                        target_bytes,
                        "handing largest micro-bucket to on_group (bounded channel; producer blocks if full)"
                    );
                    if flush_largest(&mut maps, &mut mb_bytes, &mut total_bytes).is_err() {
                        // consumer dropped rx (errored); break and let join surface it
                        break;
                    }
                    #[cfg(feature = "test-utils")]
                    if let Some(m) = buffered_bytes_metric.as_ref() {
                        m.store(total_bytes, AtomicOrdering::Relaxed);
                    }
                    if is_low_memory(cfg.hard_low_frac) {
                        sleep(Duration::from_millis(cfg.backoff_ms));
                    }
                }
            }

            // Final flush of any remaining micro-buckets.
            for i in 0..mb_count {
                if flush_bucket(i, &mut maps, &mut mb_bytes, &mut total_bytes).is_err() {
                    break;
                }
            }
            Ok(())
        })();

        // Always close tx so the consumer drains and exits.
        drop(tx);
        let consumer_result = consumer_handle.join().expect("on_group consumer panicked");
        // Surface consumer errors first (they have the actual user-callback failure).
        consumer_result?;
        producer_result?;
        Ok(())
    })
}
