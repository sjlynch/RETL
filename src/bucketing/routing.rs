use ahash::RandomState;
use anyhow::{Context, Result};
use parking_lot::Mutex;
use rayon::prelude::*;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::config::clamp_shard_count;
use crate::key_extractor::KeyExtractor;
use crate::ndjson::{read_line_capped, DEFAULT_MAX_LINE_BYTES};
use crate::zstd_jsonl::malformed_json_error;

use super::hash::{stable_index, stage1_state, stage2_state};

/// Shared shard-router used by both Stage 1 (`partition_stage1`) and
/// Stage 2 (`bucketize_shards`). Creates a validated/clamped count of
/// mutex-guarded `BufWriter<File>`s named `{file_prefix}_{:04}.jsonl` under `out_dir`,
/// then `par_iter`s over `inputs`, hashing each line's routing key with
/// `state` + [`stable_index`] and appending the line to the indexed writer.
/// All writers are flushed before return.
///
/// `state` is the per-stage `RandomState` — Stage 1 and Stage 2 must use
/// different seeds, otherwise re-bucketing in Stage 2 is a no-op.
///
/// **Dropped lines:** a line whose routing key cannot be extracted
/// ([`KeyExtractor::key_from_line`] returns `Ok(None)` — e.g. the keyed field
/// is absent or null) is dropped from the output shards. When
/// `key_extractions_failed` is `Some`, each such drop increments the counter
/// so callers can surface the loss (mirrors the dedupe stage's counter).
/// Independently of that opt-in counter, one summary `tracing::warn!` is
/// emitted before return whenever the run dropped at least one line — so the
/// loss is never fully invisible, even on the plain `partition_stage1` /
/// `bucketize_shards` entry points that pass `None`.
fn route_lines_to_shards(
    inputs: &[PathBuf],
    out_dir: &Path,
    shards: usize,
    state: RandomState,
    file_prefix: &str,
    key: &KeyExtractor,
    key_extractions_failed: Option<&AtomicU64>,
) -> Result<Vec<PathBuf>> {
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

    // Tracked unconditionally — even when the caller passed `None` for
    // `key_extractions_failed` — so the post-loop summary warning below can
    // surface an otherwise-invisible loss.
    let dropped = AtomicU64::new(0);

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
            match key
                .key_from_line(&line)
                .map_err(|e| malformed_json_error(p, line_number, e))?
            {
                Some(k) => {
                    let idx = stable_index(&state, &k, shard_count);
                    let mut w = writers[idx].lock();
                    w.write_all(line.as_bytes())?;
                    w.write_all(b"\n")?;
                }
                // No routing key: drop the line. Count it locally for the
                // summary warning, and forward to the caller's counter when a
                // `_with_key_stats` entry point supplied one.
                None => {
                    dropped.fetch_add(1, Ordering::Relaxed);
                    if let Some(counter) = key_extractions_failed {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
        Ok(())
    })?;
    for w in &writers {
        w.lock().flush()?;
    }

    // Surface key-less drops even when no external counter was supplied:
    // one summary warning per call (not per line), so e.g. bucketing a mixed
    // RC+RS corpus by `json:/parent_id` — which drops every submission — is
    // never a fully silent loss.
    let dropped = dropped.load(Ordering::Relaxed);
    if dropped > 0 {
        tracing::warn!(
            stage = file_prefix,
            dropped_lines = dropped,
            shards = shard_count,
            "bucketing dropped {} line(s) with no extractable routing key; \
             those records are absent from the output shards",
            dropped,
        );
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
///
/// **Dropped lines:** lines whose routing key cannot be extracted are dropped
/// from the output shards. A summary `tracing::warn!` fires when any line is
/// dropped; use [`partition_stage1_with_key_stats`] to also count them.
pub fn partition_stage1(
    inputs: &[PathBuf],
    out_dir: &Path,
    shards: usize,
    key: &KeyExtractor,
) -> Result<Vec<PathBuf>> {
    partition_stage1_with_key_stats(inputs, out_dir, shards, key, None)
}

/// Like [`partition_stage1`], but increments `key_extractions_failed` once for
/// every line dropped because [`KeyExtractor::key_from_line`] returned
/// `Ok(None)` (no routing key — e.g. bucketing mixed RC+RS by `json:/parent_id`
/// drops every submission). Pass `None` for the counter to opt out.
pub fn partition_stage1_with_key_stats(
    inputs: &[PathBuf],
    out_dir: &Path,
    shards: usize,
    key: &KeyExtractor,
    key_extractions_failed: Option<&AtomicU64>,
) -> Result<Vec<PathBuf>> {
    route_lines_to_shards(
        inputs,
        out_dir,
        shards,
        stage1_state(),
        "stage1",
        key,
        key_extractions_failed,
    )
}

/// Stage 2: re-bucket Stage 1 shards into `buckets` files by the **same key**.
/// Output files are named: `bucket_XXXX.jsonl`.
///
/// Parallelizes across the input shards via rayon — Stage 1 was parallelized
/// previously; Stage 2 now matches it. Drops the per-line
/// `serde_json::Value` parse (uses [`KeyExtractor::key_from_line`]) and reads
/// lines into a reusable `String` buffer.
///
/// **Dropped lines:** lines whose routing key cannot be extracted are dropped
/// from the output shards. A summary `tracing::warn!` fires when any line is
/// dropped; use [`bucketize_shards_with_key_stats`] to also count them.
pub fn bucketize_shards(
    shards: &[PathBuf],
    out_dir: &Path,
    buckets: usize,
    key: &KeyExtractor,
) -> Result<Vec<PathBuf>> {
    bucketize_shards_with_key_stats(shards, out_dir, buckets, key, None)
}

/// Like [`bucketize_shards`], but increments `key_extractions_failed` once for
/// every line dropped because [`KeyExtractor::key_from_line`] returned
/// `Ok(None)` (no routing key). Pass `None` for the counter to opt out.
pub fn bucketize_shards_with_key_stats(
    shards: &[PathBuf],
    out_dir: &Path,
    buckets: usize,
    key: &KeyExtractor,
    key_extractions_failed: Option<&AtomicU64>,
) -> Result<Vec<PathBuf>> {
    route_lines_to_shards(
        shards,
        out_dir,
        buckets,
        stage2_state(),
        "bucket",
        key,
        key_extractions_failed,
    )
}
