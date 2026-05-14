//! Generic parallel aggregation support over JSONL inputs with progress.
//! Implement `Aggregator` for your aggregation state and call `aggregate_jsonls_parallel`.

use crate::ndjson::for_each_jsonl_line_cfg;
use crate::pipeline::RedditETL;
use crate::progress::make_count_progress;
use crate::util::{
    create_dir_all_with_backoff, create_with_backoff, open_with_backoff, read_dir_with_backoff,
    remove_with_backoff, replace_file_atomic_backoff, with_thread_pool,
};
use anyhow::Result;
use indicatif::ProgressBar;
use rayon::prelude::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
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

fn shard_name_for_input(shards_dir: &Path, index: usize, input: &Path) -> PathBuf {
    let stem = input.file_stem().and_then(|s| s.to_str()).unwrap_or("part");
    // Common case: part_YYYY-MM. Keep the human-readable stem, but prefix it
    // with the input index so same-basename inputs from different directories
    // never race on the same shard or temp path.
    let stem = stem.strip_prefix("part_").unwrap_or(stem);
    shards_dir.join(format!("agg_{index:06}_{stem}.json"))
}

fn shard_names_for_inputs(shards_dir: &Path, inputs: &[PathBuf]) -> Vec<PathBuf> {
    inputs
        .iter()
        .enumerate()
        .map(|(index, input)| shard_name_for_input(shards_dir, index, input))
        .collect()
}

fn aggregate_artifact_name(path: &Path) -> Option<&str> {
    path.file_name().and_then(|s| s.to_str()).filter(|name| {
        name.starts_with("agg_") && (name.ends_with(".json") || name.ends_with(".json.inprogress"))
    })
}

fn clear_aggregate_artifacts(shards_dir: &Path) -> Result<()> {
    if !shards_dir.exists() {
        return Ok(());
    }

    for entry in read_dir_with_backoff(shards_dir, 16, 50)? {
        if !entry.file_type()?.is_file() {
            continue;
        }
        let path = entry.path();
        if aggregate_artifact_name(&path).is_some() {
            remove_with_backoff(&path, 16, 50)?;
        }
    }
    Ok(())
}

fn load_shard<A: Aggregator>(shard: &Path) -> Result<A> {
    let f = open_with_backoff(shard, 16, 50)?;
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
            if let Some(pb) = pb {
                pb.inc(1);
            }
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
        if let Some(pb) = pb {
            pb.inc(1);
        }
    }
    Ok(total)
}

/// Phase 1: build per-input aggregator shards in parallel.
///
/// Each input is ingested into a fresh `A::default()` and atomically written
/// to its precomputed shard path via the staging + rename dance. Per-input
/// failures are surfaced via `tracing::warn!` and counted, not propagated, so
/// one bad input doesn't sink the run.
///
/// Returns the exact shard paths successfully built for this input set, plus
/// the count of inputs that hit a fatal shard-build error or a tolerated
/// mid-file read error (built and will merge, but from a partial read of the
/// input).
fn build_aggregate_shards_with<A, F>(
    inputs: &[PathBuf],
    shard_paths: &[PathBuf],
    progress: bool,
    make_agg: &F,
) -> (Vec<PathBuf>, usize)
where
    A: Aggregator,
    F: Fn() -> A + Send + Sync,
{
    let pb_build = if progress {
        Some(make_count_progress(
            inputs.len() as u64,
            "Aggregate: build shards",
        ))
    } else {
        None
    };

    let shard_errors = AtomicUsize::new(0);
    let outcomes: Vec<Option<PathBuf>> = inputs
        .par_iter()
        .zip(shard_paths.par_iter())
        .map(|(input, out_shard)| {
            let tmp_shard = out_shard.with_extension("json.inprogress");

            let result = (|| -> Result<bool> {
                let mut agg = make_agg();
                let mut line_no = 0_u64;
                let had_read_error =
                    for_each_jsonl_line_cfg(input, AGGREGATE_INGEST_BUF_BYTES, |line| {
                        line_no += 1;
                        if !line.is_empty() {
                            match serde_json::from_str::<Value>(line) {
                                Ok(v) => agg.ingest(&v),
                                Err(e) => anyhow::bail!(
                                    "malformed JSON in {} at line {}: {}",
                                    input.display(),
                                    line_no,
                                    e
                                ),
                            }
                        }
                        Ok(())
                    })?;
                let out = create_with_backoff(&tmp_shard, 16, 50)?;
                let mut w = BufWriter::new(out);
                serde_json::to_writer(&mut w, &agg)?;
                w.flush()?;
                drop(w);
                replace_file_atomic_backoff(&tmp_shard, out_shard)?;
                Ok(had_read_error)
            })();

            let built = match result {
                Err(e) => {
                    shard_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        input=%input.display(),
                        shard=%out_shard.display(),
                        error=%e,
                        "failed building aggregate shard"
                    );
                    // Best-effort cleanup of any partial temp file.
                    let _ = remove_with_backoff(&tmp_shard, 16, 50);
                    None
                }
                // Shard built and will be merged, but the input was only
                // partially read (a transient I/O error was tolerated mid-file).
                // Count it so (built, errors) reflects the partial coverage.
                Ok(true) => {
                    shard_errors.fetch_add(1, Ordering::Relaxed);
                    Some(out_shard.clone())
                }
                Ok(false) => Some(out_shard.clone()),
            };

            if let Some(pb) = &pb_build {
                pb.inc(1);
            }
            built
        })
        .collect();

    if let Some(pb) = pb_build {
        pb.finish_with_message("Aggregate: shard build done");
    }

    (
        outcomes.into_iter().flatten().collect(),
        shard_errors.into_inner(),
    )
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
        resume: bool,
        pretty: bool,
    ) -> Result<(usize, usize)> {
        if resume {
            tracing::warn!(
                "aggregate_jsonls_parallel does not support resume; rebuilding aggregate shards"
            );
        }
        let (total, merged_shards, shard_errors) =
            self.aggregate_jsonls_parallel_collect::<A>(inputs, shards_dir)?;

        // Atomically publish the final output via tmp + rename so an
        // interrupted run never leaves a half-written `final_out` in place.
        let tmp_final = final_out.with_extension("json.inprogress");
        {
            let out = create_with_backoff(&tmp_final, 16, 50)?;
            let mut w = BufWriter::new(out);
            if pretty {
                serde_json::to_writer_pretty(&mut w, &total)?;
            } else {
                serde_json::to_writer(&mut w, &total)?;
            }
            w.flush()?;
        }
        replace_file_atomic_backoff(&tmp_final, final_out)?;

        Ok((merged_shards, shard_errors))
    }

    /// Build per-file aggregation shards in parallel, merge them, and return
    /// the merged aggregate state to the caller instead of publishing JSON.
    pub fn aggregate_jsonls_parallel_collect<A: Aggregator>(
        &self,
        inputs: Vec<PathBuf>,
        shards_dir: &Path,
    ) -> Result<(A, usize, usize)> {
        self.aggregate_jsonls_parallel_collect_with::<A, _>(inputs, shards_dir, A::default)
    }

    /// Like [`Self::aggregate_jsonls_parallel_collect`], but uses `make_agg` to
    /// construct per-input shard states. This lets callers provide runtime
    /// configuration while still using the [`Aggregator`] merge contract.
    pub fn aggregate_jsonls_parallel_collect_with<A, F>(
        &self,
        inputs: Vec<PathBuf>,
        shards_dir: &Path,
        make_agg: F,
    ) -> Result<(A, usize, usize)>
    where
        A: Aggregator,
        F: Fn() -> A + Send + Sync,
    {
        clear_aggregate_artifacts(shards_dir)?;
        create_dir_all_with_backoff(shards_dir, 16, 50)?;

        let shard_paths = shard_names_for_inputs(shards_dir, &inputs);

        with_thread_pool(self.opts.parallelism, || {
            let (shards, shard_errors) = build_aggregate_shards_with::<A, F>(
                &inputs,
                &shard_paths,
                self.opts.progress,
                &make_agg,
            );

            let pb_merge = if self.opts.progress {
                Some(make_count_progress(
                    shards.len() as u64,
                    "Aggregate: merge shards",
                ))
            } else {
                None
            };
            let total: A = merge_aggregator_shards_parallel(&shards, pb_merge.as_ref())?;
            if let Some(pb) = pb_merge {
                pb.finish_with_message("Aggregate: merge done");
            }

            Ok((total, shards.len(), shard_errors))
        })
    }
}
