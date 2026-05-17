use ahash::RandomState;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::fs::File;
use std::hash::{BuildHasher, Hash, Hasher};
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};
use std::thread::sleep;
use std::time::{Duration, Instant};

#[cfg(feature = "test-utils")]
use std::sync::{
    atomic::{AtomicUsize, Ordering as AtomicOrdering},
    Arc,
};

use crate::config::{clamp_shard_count, ETLOptions};
use crate::dedupe::BYTES_PER_MB;
use crate::key_extractor::KeyExtractor;
use crate::mem::{available_memory_fraction, is_low_memory, AdaptiveMemCfg};
use crate::ndjson::{read_line_capped, DEFAULT_MAX_LINE_BYTES};
use crate::util::smoothstep_memory_fraction;
use crate::zstd_jsonl::malformed_json_error;

/// Seeds for Stage 1's shard router. Distinct from [`STAGE2_SEEDS`] and
/// [`MICRO_SEEDS`] so a key landing in shard `i` here will redistribute
/// when re-bucketed in Stage 2 (and again in micro-bucketing). Do not unify.
const STAGE1_SEEDS: [u64; 4] = [
    0x1111_2222_3333_4444,
    0x5555_6666_7777_8888,
    0x9999_aaaa_bbbb_cccc,
    0xdddd_eeee_ffff_1234,
];

/// Seeds for Stage 2's re-bucketing. Intentionally differ from
/// [`STAGE1_SEEDS`] so re-bucketing actually redistributes keys across the
/// bucket files. Do not unify with the Stage 1 or micro-bucket seeds.
const STAGE2_SEEDS: [u64; 4] = [
    0xabcdef01_abcdef02,
    0xabcdef03_abcdef04,
    0xabcdef05_abcdef06,
    0xabcdef07_abcdef08,
];

/// Seeds for in-memory micro-bucket routing inside
/// [`process_bucket_streaming`]. Distinct from the Stage 1/2 seeds so the
/// final fan-out is independent of the on-disk shard/bucket assignment.
/// Do not unify with the Stage 1 or Stage 2 seeds.
const MICRO_SEEDS: [u64; 4] = [
    0x0a0b_0c0d_0e0f_a1a2,
    0xb1b2_b3b4_b5b6_c1c2,
    0xd1d2_d3d4_d5d6_e1e2,
    0xf1f2_f3f4_f5f6_0102,
];

/// Adaptive streaming configuration used during micro-bucket processing.
#[derive(Clone, Debug)]
pub struct BucketingCfg {
    /// Shared adaptive-memory policy (soft_low_frac, high_frac, adapt_cooldown_ms).
    pub mem: AdaptiveMemCfg,
    pub hard_low_frac: f64,      // when below, yield briefly
    pub backoff_ms: u64,         // sleep when under hard threshold
    pub micro_min_buf_mb: usize, // min target buffering when RAM is tight
    pub micro_max_buf_mb: usize, // max target buffering when RAM is plentiful
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
            inflight_bytes: 256 * BYTES_PER_MB,
            inflight_groups: 8,
        }
    }
}

impl From<&ETLOptions> for BucketingCfg {
    fn from(opts: &ETLOptions) -> Self {
        crate::config::warn_if_inflight_pair_pathological(
            opts.inflight_bytes,
            opts.inflight_groups,
        );
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
/// Stage 2 (`bucketize_shards`). Creates a validated/clamped count of
/// mutex-guarded `BufWriter<File>`s named `{file_prefix}_{:04}.jsonl` under `out_dir`,
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

    crate::util::create_dir_all_with_default_backoff(out_dir)
        .with_context(|| format!("create shard output dir {}", out_dir.display()))?;

    let shard_count = clamp_shard_count(shards, "bucketing::route_lines_to_shards");
    let mut writers: Vec<Mutex<BufWriter<File>>> = Vec::with_capacity(shard_count);
    let mut paths: Vec<PathBuf> = Vec::with_capacity(shard_count);
    for i in 0..shard_count {
        let p = out_dir.join(format!("{}_{:04}.jsonl", file_prefix, i));
        let f = crate::util::create_with_default_backoff(&p)
            .with_context(|| format!("create {}", p.display()))?;
        writers.push(Mutex::new(BufWriter::new(f)));
        paths.push(p);
    }

    inputs.par_iter().try_for_each(|p| -> Result<()> {
        let f = crate::util::open_with_default_backoff(p)
            .with_context(|| format!("open {}", p.display()))?;
        let mut r = BufReader::new(f);
        // Reusable line buffer — avoids the per-line String allocation from
        // `BufReader::lines()` (mirrors `zstd_jsonl::for_each_line_attempt`).
        let mut line = String::with_capacity(16 * 1024);
        let mut line_number: u64 = 0;
        loop {
            let n = read_line_capped(&mut r, &mut line, DEFAULT_MAX_LINE_BYTES, p).with_context(
                || {
                    format!(
                        "read bucketing shard input {} near line {}",
                        p.display(),
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
            if let Some(k) = key
                .key_from_line(&line)
                .map_err(|e| malformed_json_error(p, line_number, e))?
            {
                let idx = stable_index(&state, &k, shard_count);
                let mut w = writers[idx].lock();
                w.write_all(line.as_bytes())?;
                w.write_all(b"\n")?;
            }
        }
        Ok(())
    })?;
    for w in &writers {
        w.lock().flush()?;
    }
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
    let rs = RandomState::with_seeds(
        STAGE1_SEEDS[0],
        STAGE1_SEEDS[1],
        STAGE1_SEEDS[2],
        STAGE1_SEEDS[3],
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
    let rs = RandomState::with_seeds(
        STAGE2_SEEDS[0],
        STAGE2_SEEDS[1],
        STAGE2_SEEDS[2],
        STAGE2_SEEDS[3],
    );
    route_lines_to_shards(shards, out_dir, buckets, rs, "bucket", key)
}

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

    /// Drain micro-bucket `idx`, zero its byte counters, and ship every
    /// `(key, group)` over `tx`. Returns `Err(())` if the consumer dropped
    /// the receiver — the producer breaks and lets the join surface the
    /// underlying error.
    fn flush_bucket(
        &mut self,
        idx: usize,
        tx: &crossbeam_channel::Sender<(String, Vec<String>)>,
    ) -> std::result::Result<(), ()> {
        if self.maps[idx].is_empty() {
            return Ok(());
        }
        let mut m = HashMap::new();
        std::mem::swap(&mut m, &mut self.maps[idx]);
        let used = self.mb_bytes[idx];
        self.mb_bytes[idx] = 0;
        if self.total_bytes >= used {
            self.total_bytes -= used;
        } else {
            self.total_bytes = 0;
        }
        for (k, v) in m.into_iter() {
            tx.send((k, v)).map_err(|_| ())?;
        }
        Ok(())
    }

    /// Find the micro-bucket holding the most bytes and flush it via
    /// [`Self::flush_bucket`]. No-op when every bucket is empty.
    fn flush_largest(
        &mut self,
        tx: &crossbeam_channel::Sender<(String, Vec<String>)>,
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
            self.flush_bucket(max_idx, tx)?;
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
            if state.flush_largest(tx).is_err() {
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
        if state.flush_bucket(i, tx).is_err() {
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

    let rs = RandomState::with_seeds(
        MICRO_SEEDS[0],
        MICRO_SEEDS[1],
        MICRO_SEEDS[2],
        MICRO_SEEDS[3],
    );

    let mut state = MicroBucketState::with_capacity(micro_buckets);

    // Per-flush byte cap: with channel cap = inflight_groups, total inflight
    // bytes are bounded by 2 * per_flush_cap (one growing producer-side bucket
    // + the channel + the consumer's current group).
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
