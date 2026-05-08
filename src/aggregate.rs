//! Generic parallel aggregation support over JSONL inputs with progress.
//! Implement `Aggregator` for your aggregation state and call `aggregate_jsonls_parallel`.

use crate::progress::make_count_progress;
use crate::pipeline::RedditETL;
use crate::util::replace_file_atomic_backoff;
use anyhow::Result;
use indicatif::ProgressBar;
use rayon::prelude::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

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

impl RedditETL {
    /// Build per-file aggregation shards in parallel, then merge into `final_out`.
    /// - Always rebuilds shards (no resume behavior).
    /// - `pretty == true` pretty-prints the final output (shards are compact).
    ///
    /// Returns `(merged_shards, shard_errors)`: the number of shards that were
    /// successfully built and folded into the final output, and the number of
    /// inputs whose shard build failed (and were therefore dropped from the
    /// aggregate). Failures are logged via `tracing::warn!`.
    pub fn aggregate_jsonls_parallel<A: Aggregator>(
        &self,
        inputs: Vec<PathBuf>,
        shards_dir: &Path,
        final_out: &Path,
        _resume: bool,
        pretty: bool,
    ) -> Result<(usize, usize)> {
        fs::create_dir_all(shards_dir)?;

        let pb_build = if self.opts.progress { Some(make_count_progress(inputs.len() as u64, "Aggregate: build shards")) } else { None };

        // Build shards in parallel (always rebuild). Per-input failures are
        // surfaced via tracing and counted, not silently swallowed.
        let shard_errors = AtomicUsize::new(0);
        inputs.par_iter().for_each(|input| {
            let out_shard = shard_name_for_input(shards_dir, input);
            let tmp_shard = out_shard.with_extension("json.inprogress");

            let result = (|| -> Result<()> {
                let mut agg = A::default();
                let f = File::open(input)?;
                let r = BufReader::new(f);
                for line in r.lines() {
                    let line = line?;
                    if line.is_empty() { continue; }
                    if let Ok(v) = serde_json::from_str::<Value>(&line) {
                        agg.ingest(&v);
                    }
                }
                let out = File::create(&tmp_shard)?;
                let mut w = BufWriter::new(out);
                serde_json::to_writer(&mut w, &agg)?;
                w.flush()?;
                drop(w);
                replace_file_atomic_backoff(&tmp_shard, &out_shard)?;
                Ok(())
            })();

            if let Err(e) = result {
                shard_errors.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(path=%out_shard.display(), error=%e, "failed building aggregate shard");
                // Best-effort cleanup of any partial temp file.
                let _ = fs::remove_file(&tmp_shard);
            }

            if let Some(pb) = &pb_build { pb.inc(1); }
        });
        let shard_errors = shard_errors.into_inner();

        if let Some(pb) = pb_build { pb.finish_with_message("Aggregate: shard build done"); }

        // Merge shards. Only consider successfully-written shard files
        // (skip any leftover `.inprogress` temps from prior failures).
        let mut shards: Vec<PathBuf> = fs::read_dir(shards_dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("json"))
            .collect();
        shards.sort();

        let pb_merge = if self.opts.progress { Some(make_count_progress(shards.len() as u64, "Aggregate: merge shards")) } else { None };

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
        Ok((merged_shards, shard_errors))
    }
}
