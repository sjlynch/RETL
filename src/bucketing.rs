use anyhow::{Context, Result};
use ahash::RandomState;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::hash::{BuildHasher, Hash, Hasher};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::thread::sleep;
use std::time::{Duration, Instant};

use crate::mem::{available_memory_fraction, is_low_memory};
use crate::util::open_with_backoff;

/// Adaptive streaming configuration used during micro-bucket processing.
#[derive(Clone, Debug)]
pub struct BucketingCfg {
    pub soft_low_frac: f64,       // when below, prefer smaller buffers
    pub hard_low_frac: f64,       // when below, yield briefly
    pub high_frac: f64,           // when above, allow larger buffers
    pub backoff_ms: u64,          // sleep when under hard threshold
    pub micro_min_buf_mb: usize,  // min target buffering when RAM is tight
    pub micro_max_buf_mb: usize,  // max target buffering when RAM is plentiful
    pub adapt_cooldown_ms: u64,   // recompute buffer target at this cadence
}

impl Default for BucketingCfg {
    fn default() -> Self {
        Self {
            soft_low_frac: 0.18,
            hard_low_frac: 0.10,
            high_frac: 0.85,
            backoff_ms: 25,
            micro_min_buf_mb: 128,
            micro_max_buf_mb: 4096,
            adapt_cooldown_ms: 400,
        }
    }
}

#[inline]
fn stable_index(state: &RandomState, key: &str, parts: usize) -> usize {
    let mut h = state.build_hasher();
    key.hash(&mut h);
    (h.finish() as usize) % parts.max(1)
}

/// Stage 1: shard arbitrary NDJSON inputs by a key extractor into `shards` files.
/// Output files are named: `stage1_XXXX.jsonl`.
///
/// `extract_key(&Value) -> Option<String>` decides which key to route on (e.g., author).
pub fn partition_stage1<E>(
    inputs: &[PathBuf],
    out_dir: &Path,
    shards: usize,
    extract_key: E,
) -> Result<Vec<PathBuf>>
where
    E: Sync + Fn(&Value) -> Option<String>,
{
    use parking_lot::Mutex;
    use std::fs::File;
    use std::io::{BufReader, BufWriter};

    fs::create_dir_all(out_dir)?;

    // Deterministic sharding state for Stage 1.
    let rs = RandomState::with_seeds(
        0x1111_2222_3333_4444,
        0x5555_6666_7777_8888,
        0x9999_aaaa_bbbb_cccc,
        0xdddd_eeee_ffff_1234,
    );

    let mut writers: Vec<Mutex<BufWriter<File>>> = Vec::with_capacity(shards);
    let mut paths: Vec<PathBuf> = Vec::with_capacity(shards);
    for i in 0..shards.max(1) {
        let p = out_dir.join(format!("stage1_{:04}.jsonl", i));
        let f = std::fs::File::create(&p).with_context(|| format!("create {}", p.display()))?;
        writers.push(Mutex::new(BufWriter::new(f)));
        paths.push(p);
    }

    for p in inputs {
        let f = open_with_backoff(p, 16, 50).with_context(|| format!("open {}", p.display()))?;
        let r = BufReader::new(f);
        for line in r.lines() {
            let line = line?;
            if line.is_empty() { continue; }
            if let Ok(v) = serde_json::from_str::<Value>(&line) {
                if let Some(key) = extract_key(&v) {
                    let idx = stable_index(&rs, &key, shards);
                    let mut w = writers[idx].lock();
                    w.write_all(line.as_bytes())?;
                    w.write_all(b"\n")?;
                }
            }
        }
    }
    for w in &writers { w.lock().flush()?; }
    Ok(paths)
}

/// Stage 2: re-bucket a Stage 1 shard into `buckets` files by the **same key extractor**.
/// Output files are named: `bucket_XXXX.jsonl`.
pub fn bucketize_shard<E>(shard: &Path, out_dir: &Path, buckets: usize, extract_key: E) -> Result<Vec<PathBuf>>
where
    E: Fn(&Value) -> Option<String>,
{
    use parking_lot::Mutex;
    use std::fs::File;
    use std::io::{BufReader, BufWriter};

    fs::create_dir_all(out_dir)?;

    // Deterministic sharding for Stage 2.
    let rs = RandomState::with_seeds(
        0xabcdef01_abcdef02,
        0xabcdef03_abcdef04,
        0xabcdef05_abcdef06,
        0xabcdef07_abcdef08,
    );

    let mut writers: Vec<Mutex<BufWriter<File>>> = Vec::with_capacity(buckets);
    let mut paths: Vec<PathBuf> = Vec::with_capacity(buckets);
    for i in 0..buckets.max(1) {
        let p = out_dir.join(format!("bucket_{:04}.jsonl", i));
        let f = std::fs::File::create(&p).with_context(|| format!("create {}", p.display()))?;
        writers.push(Mutex::new(BufWriter::new(f)));
        paths.push(p);
    }

    let f = open_with_backoff(shard, 16, 50).with_context(|| format!("open {}", shard.display()))?;
    let r = BufReader::new(f);
    for line in r.lines() {
        let line = line?;
        if line.is_empty() { continue; }
        if let Ok(v) = serde_json::from_str::<Value>(&line) {
            if let Some(key) = extract_key(&v) {
                let idx = stable_index(&rs, &key, buckets);
                let mut w = writers[idx].lock();
                w.write_all(line.as_bytes())?;
                w.write_all(b"\n")?;
            }
        }
    }
    for w in &writers { w.lock().flush()?; }
    Ok(paths)
}

/// Stage 3: **in-memory** micro-bucket streaming with adaptive memory.
/// Reads `bucket_XXXX.jsonl`, routes lines to `micro_buckets` in memory using a
/// deterministic hasher, and **flushes groups adaptively** to the callback.
///
/// The callback receives `(key, Vec<String>)` for each group. When RAM is tight,
/// a given key may be flushed **multiple times** (partial groups); design your
/// downstream to merge if needed (our Stage 6 merge does this).
///
/// `extract_key` must be consistent with Stage 1/2 to preserve routing consistency.
pub fn process_bucket_streaming<F, E>(
    bucket: &Path,
    micro_buckets: usize,
    cfg: &BucketingCfg,
    mut on_group: F,
    extract_key: E,
) -> Result<()>
where
    F: FnMut(&str, Vec<String>) -> Result<()>,
    E: Fn(&Value) -> Option<String>,
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
    let r = BufReader::new(file);

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

    // Helper: flush one micro-bucket (by index).
    let mut flush_bucket = |idx: usize,
                            maps: &mut [HashMap<String, Vec<String>>],
                            mb_bytes: &mut [usize],
                            total_bytes: &mut usize| -> Result<()> {
        if maps[idx].is_empty() { return Ok(()); }
        let mut m = HashMap::new();
        std::mem::swap(&mut m, &mut maps[idx]);
        let used = mb_bytes[idx];
        mb_bytes[idx] = 0;
        if *total_bytes >= used { *total_bytes -= used; } else { *total_bytes = 0; }

        for (k, v) in m.into_iter() {
            on_group(&k, v)?;
        }
        Ok(())
    };

    // Helper: flush the largest bucket to quickly free memory.
    let mut flush_largest = |maps: &mut [HashMap<String, Vec<String>>],
                             mb_bytes: &mut [usize],
                             total_bytes: &mut usize| -> Result<()> {
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

    for line in r.lines() {
        let line = line?;
        if line.is_empty() { continue; }
        let v: Value = match serde_json::from_str(&line) { Ok(x) => x, Err(_) => continue };

        let key = match extract_key(&v) { Some(x) => x, None => continue };
        let idx = stable_index(&rs, &key, mb_count);

        let e = maps[idx].entry(key).or_default();
        e.push(line.clone());

        let add = line.len() + 1;
        mb_bytes[idx] += add;
        total_bytes += add;

        // Periodically recompute target based on current free memory.
        if last_eval.elapsed() >= Duration::from_millis(cfg.adapt_cooldown_ms) {
            let free = available_memory_fraction();
            let span = (cfg.high_frac - cfg.soft_low_frac).max(0.05f64);
            let mut scale = ((free - cfg.soft_low_frac) / span).clamp(0.0, 1.0);
            // smootherstep
            scale = scale * scale * (3.0 - 2.0 * scale);
            target_bytes = ((cfg.micro_min_buf_mb as f64
                + (cfg.micro_max_buf_mb as f64 - cfg.micro_min_buf_mb as f64) * scale)
                .round() as usize) * 1024 * 1024;
            last_eval = Instant::now();
        }

        // Adaptive flush:
        if total_bytes >= target_bytes || is_low_memory(cfg.soft_low_frac) {
            flush_largest(&mut maps, &mut mb_bytes, &mut total_bytes)?;
            if is_low_memory(cfg.hard_low_frac) {
                sleep(Duration::from_millis(cfg.backoff_ms));
            }
        }
    }

    // Final flush of any remaining micro-buckets.
    for i in 0..mb_count {
        flush_bucket(i, &mut maps, &mut mb_bytes, &mut total_bytes)?;
    }

    Ok(())
}
