//! Generic parallel aggregation support over JSONL inputs with progress.
//! Implement `Aggregator` for your aggregation state and call `aggregate_jsonls_parallel`.

use crate::progress::make_count_progress;
use crate::pipeline::RedditETL;
use crate::ndjson::for_each_jsonl_line_cfg;
use crate::util::replace_file_atomic_backoff;
use anyhow::Result;
use indicatif::ProgressBar;
use rayon::prelude::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use std::fs;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Read buffer for ingesting JSONL into per-shard aggregate state. Smaller
/// than the corpus-scan buffer because aggregate inputs are stitched JSONL
/// (one record per line, already filtered) — large reads add latency without
/// throughput.
const AGGREGATE_INGEST_BUF_BYTES: usize = 16 * 1024;

/// State that ingests JSON records and folds together with peer states.
///
/// `merge` MUST be associative: for any states `a`, `b`, `c`,
/// `(a.merge(b)).merge(c)` and `a.merge(b.merge(c))` must produce equal
/// final states. Shard merging uses `rayon`'s tree reduction, which splits
/// the input into adjacent ranges and combines partial results in arbitrary
/// nesting — non-associative `merge` impls will produce nondeterministic
/// output. Commutativity is *not* required: adjacent shards are always
/// combined in left-to-right order.
pub trait Aggregator: Send + Default + Serialize + DeserializeOwned {
    fn ingest(&mut self, record: &Value);
    fn merge(&mut self, other: Self);
}

fn shard_name_for_input(shards_dir: &Path, input: &Path) -> PathBuf {
    let stem = input.file_stem().and_then(|s| s.to_str()).unwrap_or("part");
    // Common case: part_YYYY-MM
    let stem = stem.strip_prefix("part_").unwrap_or(stem);
    shards_dir.join(format!("agg_{}.json", stem))
}

fn load_shard<A: Aggregator>(shard: &Path) -> Result<A> {
    let f = File::open(shard)?;
    let r = BufReader::new(f);
    let part: A = serde_json::from_reader(r)?;
    Ok(part)
}

/// Parallel tree-merge of aggregator shards from disk.
/// `pb`, if provided, is incremented once per shard as it's loaded.
///
/// Uses `try_fold` + `try_reduce` so each rayon worker accumulates an
/// entire chunk locally (no cross-thread synchronization per shard), then
/// only the per-worker totals are combined via tree reduction.
#[doc(hidden)]
pub fn merge_aggregator_shards_parallel<A: Aggregator>(
    shards: &[PathBuf],
    pb: Option<&ProgressBar>,
) -> Result<A> {
    shards
        .par_iter()
        .try_fold(A::default, |mut acc, shard| -> Result<A> {
            let part: A = load_shard(shard)?;
            acc.merge(part);
            if let Some(pb) = pb { pb.inc(1); }
            Ok(acc)
        })
        .try_reduce(A::default, |mut left, right| {
            left.merge(right);
            Ok(left)
        })
}

/// Serial fold of aggregator shards from disk. Kept for benchmark
/// comparisons against the parallel path.
#[doc(hidden)]
pub fn merge_aggregator_shards_serial<A: Aggregator>(
    shards: &[PathBuf],
    pb: Option<&ProgressBar>,
) -> Result<A> {
    let mut total = A::default();
    for shard in shards {
        let part: A = load_shard(shard)?;
        total.merge(part);
        if let Some(pb) = pb { pb.inc(1); }
    }
    Ok(total)
}

/// Phase 1: build per-input aggregator shards in parallel.
///
/// Each input is ingested into a fresh `A::default()` and atomically written
/// to `<shards_dir>/agg_<stem>.json` via the staging + rename dance. Per-input
/// failures are surfaced via `tracing::warn!` and counted, not propagated, so
/// one bad input doesn't sink the run.
///
/// Returns the count of inputs that hit a fatal shard-build error or a
/// tolerated mid-file read error (built and will merge, but from a partial
/// read of the input).
fn build_aggregate_shards<A: Aggregator>(
    inputs: &[PathBuf],
    shards_dir: &Path,
    progress: bool,
) -> usize {
    let pb_build = if progress {
        Some(make_count_progress(inputs.len() as u64, "Aggregate: build shards"))
    } else {
        None
    };

    let shard_errors = AtomicUsize::new(0);
    inputs.par_iter().for_each(|input| {
        let out_shard = shard_name_for_input(shards_dir, input);
        let tmp_shard = out_shard.with_extension("json.inprogress");

        let result = (|| -> Result<bool> {
            let mut agg = A::default();
            let had_read_error = for_each_jsonl_line_cfg(input, AGGREGATE_INGEST_BUF_BYTES, |line| {
                if !line.is_empty() {
                    if let Ok(v) = serde_json::from_str::<Value>(line) {
                        agg.ingest(&v);
                    }
                }
                Ok(())
            })?;
            let out = File::create(&tmp_shard)?;
            let mut w = BufWriter::new(out);
            serde_json::to_writer(&mut w, &agg)?;
            w.flush()?;
            drop(w);
            replace_file_atomic_backoff(&tmp_shard, &out_shard)?;
            Ok(had_read_error)
        })();

        match result {
            Err(e) => {
                shard_errors.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(path=%out_shard.display(), error=%e, "failed building aggregate shard");
                // Best-effort cleanup of any partial temp file.
                let _ = fs::remove_file(&tmp_shard);
            }
            // Shard built and will be merged, but the input was only
            // partially read (a transient I/O error was tolerated mid-file).
            // Count it so (built, errors) reflects the partial coverage.
            Ok(true) => {
                shard_errors.fetch_add(1, Ordering::Relaxed);
            }
            Ok(false) => {}
        }

        if let Some(pb) = &pb_build { pb.inc(1); }
    });

    if let Some(pb) = pb_build { pb.finish_with_message("Aggregate: shard build done"); }

    shard_errors.into_inner()
}

/// Phases 2 + 3: discover shards in `shards_dir`, tree-merge them, and
/// atomically publish the merged result to `final_out`.
///
/// Only `*.json` shards are considered — leftover `*.inprogress` temps from
/// prior failures are skipped. The final output goes through the same
/// staging + `replace_file_atomic_backoff` dance as the shards so an
/// interrupted run never leaves a half-written `final_out` in place.
///
/// Returns the number of shards successfully folded into the final output.
fn publish_aggregated_final<A: Aggregator>(
    shards_dir: &Path,
    final_out: &Path,
    pretty: bool,
    progress: bool,
) -> Result<usize> {
    // Merge shards. Only consider successfully-written shard files
    // (skip any leftover `.inprogress` temps from prior failures).
    let mut shards: Vec<PathBuf> = fs::read_dir(shards_dir)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("json"))
        .collect();
    shards.sort();

    let pb_merge = if progress {
        Some(make_count_progress(shards.len() as u64, "Aggregate: merge shards"))
    } else {
        None
    };

    // Tree-merge shards in parallel. `Aggregator::merge` is required to
    // be associative (see trait docs); rayon's reduction preserves the
    // left-to-right ordering of adjacent shards but nests combines
    // arbitrarily.
    let total: A = merge_aggregator_shards_parallel(&shards, pb_merge.as_ref())?;
    let merged_shards = shards.len();

    // Atomically publish the final output via tmp + rename so an
    // interrupted run never leaves a half-written `final_out` in place.
    let tmp_final = final_out.with_extension("json.inprogress");
    {
        let out = File::create(&tmp_final)?;
        let mut w = BufWriter::new(out);
        if pretty {
            serde_json::to_writer_pretty(&mut w, &total)?;
        } else {
            serde_json::to_writer(&mut w, &total)?;
        }
        w.flush()?;
    }
    replace_file_atomic_backoff(&tmp_final, final_out)?;

    if let Some(pb) = pb_merge { pb.finish_with_message("Aggregate: final written"); }
    Ok(merged_shards)
}

impl RedditETL {
    /// Build per-file aggregation shards in parallel, then merge into `final_out`.
    /// - Always rebuilds shards (no resume behavior).
    /// - `pretty == true` pretty-prints the final output (shards are compact).
    ///
    /// Returns `(merged_shards, shard_errors)`: the number of shards that were
    /// successfully built and folded into the final output, and the number of
    /// inputs whose shard build had errors — either fatal (dropped from the
    /// aggregate) or a tolerated mid-file read error (built and merged from a
    /// partial read of the input). Failures are logged via `tracing::warn!`.
    pub fn aggregate_jsonls_parallel<A: Aggregator>(
        &self,
        inputs: Vec<PathBuf>,
        shards_dir: &Path,
        final_out: &Path,
        _resume: bool,
        pretty: bool,
    ) -> Result<(usize, usize)> {
        fs::create_dir_all(shards_dir)?;
        let shard_errors = build_aggregate_shards::<A>(&inputs, shards_dir, self.opts.progress);
        let merged_shards = publish_aggregated_final::<A>(shards_dir, final_out, pretty, self.opts.progress)?;
        Ok((merged_shards, shard_errors))
    }
}
