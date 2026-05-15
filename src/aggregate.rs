//! Generic parallel aggregation support over JSONL inputs with progress.
//! Implement `Aggregator` for your aggregation state and call `aggregate_jsonls_parallel`.

use crate::atomic_write::{ensure_staging_dir, write_jsonl_atomic};
use crate::ndjson::for_each_jsonl_line_cfg;
use crate::pipeline::RedditETL;
use crate::progress::make_count_progress;
use crate::util::{create_dir_all_with_backoff, open_with_backoff, with_thread_pool};
use anyhow::{Context, Result};
use indicatif::ProgressBar;
use rayon::prelude::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Read buffer for ingesting JSONL into per-shard aggregate state. Smaller
/// than the corpus-scan buffer because aggregate inputs are stitched JSONL
/// (one record per line, already filtered) — large reads add latency without
/// throughput.
const AGGREGATE_INGEST_BUF_BYTES: usize = 16 * 1024;
const AGGREGATE_WRITE_BUF_BYTES: usize = 64 * 1024;
static AGGREGATE_RUN_COUNTER: AtomicU64 = AtomicU64::new(0);

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

/// Per-input aggregate issue returned to library callers and printed by the
/// CLI. `input` is the path supplied by the caller; `error` is a copy/pasteable
/// diagnostic string.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateInputIssue {
    pub input: PathBuf,
    pub error: String,
}

impl AggregateInputIssue {
    fn new(input: &Path, error: impl ToString) -> Self {
        Self {
            input: input.to_path_buf(),
            error: error.to_string(),
        }
    }
}

/// Policy for a mid-file JSONL read error after some records were already
/// ingested from an input.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AggregatePartialReadPolicy {
    /// Drop the partially read input from the merged aggregate and report it
    /// in [`AggregateBuildReport::partial_inputs`]. This is the default so a
    /// transient read failure cannot silently produce a partial rollup.
    #[default]
    Strict,
    /// Write and merge the partial shard, while still reporting the input in
    /// [`AggregateBuildReport::partial_inputs`]. This preserves the historical
    /// tolerant behavior for library callers that explicitly opt in.
    MergePartial,
}

/// Summary of the aggregate shard-build phase.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AggregateBuildReport {
    /// Inputs that were read cleanly and whose shards were merged.
    pub ok_inputs: Vec<PathBuf>,
    /// Inputs that hit a mid-file read error. Under the default strict policy
    /// these inputs are not merged; under [`AggregatePartialReadPolicy::MergePartial`]
    /// they are merged but still reported here.
    pub partial_inputs: Vec<AggregateInputIssue>,
    /// Inputs that failed before producing a usable shard (open errors,
    /// malformed JSON, shard write/publish errors, etc.).
    pub fatal_inputs: Vec<AggregateInputIssue>,
    /// Number of shard files actually folded into the returned aggregate.
    pub merged_shards: usize,
}

impl AggregateBuildReport {
    pub fn partial_count(&self) -> usize {
        self.partial_inputs.len()
    }

    pub fn fatal_count(&self) -> usize {
        self.fatal_inputs.len()
    }

    pub fn problem_count(&self) -> usize {
        self.partial_count() + self.fatal_count()
    }

    pub fn has_problems(&self) -> bool {
        self.problem_count() > 0
    }
}

fn aggregate_run_token() -> String {
    let counter = AGGREGATE_RUN_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("p{}_r{counter}_t{nanos}", std::process::id())
}

fn output_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn aggregate_run_dir(shards_dir: &Path, run_token: &str) -> PathBuf {
    shards_dir.join(format!("run_{run_token}"))
}

fn shard_name_for_input(run_dir: &Path, run_token: &str, index: usize, input: &Path) -> PathBuf {
    let stem = input.file_stem().and_then(|s| s.to_str()).unwrap_or("part");
    // Common case: part_YYYY-MM. Keep the human-readable stem, but prefix it
    // with the run token and input index so same-basename inputs from different
    // directories are distinct. The enclosing per-run directory isolates live
    // artifacts from other aggregate jobs sharing the same caller-provided
    // shards_dir.
    let stem = stem.strip_prefix("part_").unwrap_or(stem);
    run_dir.join(format!("agg_{run_token}_{index:06}_{stem}.json"))
}

fn shard_names_for_inputs(run_dir: &Path, run_token: &str, inputs: &[PathBuf]) -> Vec<PathBuf> {
    inputs
        .iter()
        .enumerate()
        .map(|(index, input)| shard_name_for_input(run_dir, run_token, index, input))
        .collect()
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

struct BuildOutcome {
    shards: Vec<PathBuf>,
    report: AggregateBuildReport,
}

enum ShardBuildResult {
    Clean {
        input: PathBuf,
        shard: PathBuf,
    },
    Partial {
        input: PathBuf,
        error: String,
        shard: Option<PathBuf>,
    },
    Fatal {
        input: PathBuf,
        error: String,
    },
}

fn write_shard<A: Aggregator>(staging_dir: &Path, out_shard: &Path, agg: &A) -> Result<()> {
    write_jsonl_atomic(staging_dir, out_shard, AGGREGATE_WRITE_BUF_BYTES, |w| {
        serde_json::to_writer(w, agg)?;
        Ok(())
    })
    .with_context(|| format!("write aggregate shard {}", out_shard.display()))
}

/// Phase 1: build per-input aggregator shards in parallel.
///
/// Each input is ingested into a fresh `make_agg()` state and atomically
/// written to its precomputed shard path via the staging + rename dance.
/// Fatal per-input failures are surfaced via `tracing::warn!` and collected,
/// not propagated, so one bad input doesn't sink the entire run. Mid-file
/// read errors are collected separately; by default their partial state is
/// dropped instead of merged.
fn build_aggregate_shards_with<A, F>(
    inputs: &[PathBuf],
    shard_paths: &[PathBuf],
    staging_dir: &Path,
    progress: bool,
    make_agg: &F,
    partial_policy: AggregatePartialReadPolicy,
) -> BuildOutcome
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

    let outcomes: Vec<ShardBuildResult> = inputs
        .par_iter()
        .zip(shard_paths.par_iter())
        .map(|(input, out_shard)| {
            let mut agg = make_agg();
            let mut line_no = 0_u64;
            let ingest_result = for_each_jsonl_line_cfg(input, AGGREGATE_INGEST_BUF_BYTES, |line| {
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
            });

            let outcome = match ingest_result {
                Err(e) => {
                    tracing::warn!(
                        input=%input.display(),
                        shard=%out_shard.display(),
                        error=%e,
                        "failed building aggregate shard"
                    );
                    ShardBuildResult::Fatal {
                        input: input.clone(),
                        error: e.to_string(),
                    }
                }
                Ok(read_error) => {
                    let partial_error = read_error.as_ref().map(|e| e.to_string());
                    if partial_error.is_some()
                        && partial_policy == AggregatePartialReadPolicy::Strict
                    {
                        let error = partial_error.expect("checked is_some");
                        tracing::warn!(
                            input=%input.display(),
                            error=%error,
                            "partial aggregate input read; dropping partial shard"
                        );
                        ShardBuildResult::Partial {
                            input: input.clone(),
                            error,
                            shard: None,
                        }
                    } else {
                        match write_shard(staging_dir, out_shard, &agg) {
                            Ok(()) => {
                                if let Some(error) = partial_error {
                                    tracing::warn!(
                                        input=%input.display(),
                                        shard=%out_shard.display(),
                                        error=%error,
                                        "partial aggregate input read; merging partial shard by policy"
                                    );
                                    ShardBuildResult::Partial {
                                        input: input.clone(),
                                        error,
                                        shard: Some(out_shard.clone()),
                                    }
                                } else {
                                    ShardBuildResult::Clean {
                                        input: input.clone(),
                                        shard: out_shard.clone(),
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    input=%input.display(),
                                    shard=%out_shard.display(),
                                    error=%e,
                                    "failed building aggregate shard"
                                );
                                ShardBuildResult::Fatal {
                                    input: input.clone(),
                                    error: e.to_string(),
                                }
                            }
                        }
                    }
                }
            };

            if let Some(pb) = &pb_build {
                pb.inc(1);
            }
            outcome
        })
        .collect();

    if let Some(pb) = pb_build {
        pb.finish_with_message("Aggregate: shard build done");
    }

    let mut shards = Vec::new();
    let mut report = AggregateBuildReport::default();
    for outcome in outcomes {
        match outcome {
            ShardBuildResult::Clean { input, shard } => {
                report.ok_inputs.push(input);
                shards.push(shard);
            }
            ShardBuildResult::Partial {
                input,
                error,
                shard,
            } => {
                if let Some(shard) = shard {
                    shards.push(shard);
                }
                report
                    .partial_inputs
                    .push(AggregateInputIssue::new(&input, error));
            }
            ShardBuildResult::Fatal { input, error } => {
                report
                    .fatal_inputs
                    .push(AggregateInputIssue::new(&input, error));
            }
        }
    }
    report.merged_shards = shards.len();

    BuildOutcome { shards, report }
}

impl RedditETL {
    /// Build per-file aggregation shards in parallel, then merge into `final_out`.
    /// - Always rebuilds shards in a fresh per-run directory under `shards_dir`
    ///   (aggregate has no resume behavior).
    /// - `pretty == true` field-indents the final JSON output (shards are compact).
    /// - Mid-file JSONL read errors are strict by default: the partial input is
    ///   reported and not merged. Use
    ///   [`Self::aggregate_jsonls_parallel_collect_with_policy`] with
    ///   [`AggregatePartialReadPolicy::MergePartial`] to explicitly opt into
    ///   historical tolerant merging.
    ///
    /// Returns an [`AggregateBuildReport`] naming clean, partial, and fatal
    /// inputs. If every non-empty input fails to produce a merged shard, no
    /// final output is published and an error is returned.
    pub fn aggregate_jsonls_parallel<A: Aggregator>(
        &self,
        inputs: Vec<PathBuf>,
        shards_dir: &Path,
        final_out: &Path,
        pretty: bool,
    ) -> Result<AggregateBuildReport> {
        let input_count = inputs.len();
        let (total, report) = self.aggregate_jsonls_parallel_collect::<A>(inputs, shards_dir)?;
        if input_count > 0 && report.merged_shards == 0 && report.problem_count() > 0 {
            anyhow::bail!(
                "aggregate failed: {} of {} input(s) failed or were partial; 0 shard(s) merged",
                report.problem_count(),
                input_count
            );
        }

        // Atomically publish the final output through `<out-parent>/_staging`
        // so an interrupted run never leaves a half-written `final_out` in
        // place and concurrent runs never share a fixed temp path.
        let staging_dir = ensure_staging_dir(output_parent(final_out))?;
        write_jsonl_atomic(&staging_dir, final_out, AGGREGATE_WRITE_BUF_BYTES, |w| {
            if pretty {
                serde_json::to_writer_pretty(w, &total)?;
            } else {
                serde_json::to_writer(w, &total)?;
            }
            Ok(())
        })
        .with_context(|| format!("publishing aggregate output {}", final_out.display()))?;

        Ok(report)
    }

    /// Build per-file aggregation shards in a fresh per-run directory under
    /// `shards_dir`, merge them, and return the merged aggregate state to the
    /// caller instead of publishing JSON.
    ///
    /// Uses [`AggregatePartialReadPolicy::Strict`], so partial reads are
    /// reported but not merged.
    pub fn aggregate_jsonls_parallel_collect<A: Aggregator>(
        &self,
        inputs: Vec<PathBuf>,
        shards_dir: &Path,
    ) -> Result<(A, AggregateBuildReport)> {
        self.aggregate_jsonls_parallel_collect_with::<A, _>(inputs, shards_dir, A::default)
    }

    /// Like [`Self::aggregate_jsonls_parallel_collect`], but uses `make_agg` to
    /// construct per-input shard states. This lets callers provide runtime
    /// configuration while still using the [`Aggregator`] merge contract.
    ///
    /// Uses [`AggregatePartialReadPolicy::Strict`], so partial reads are
    /// reported but not merged.
    pub fn aggregate_jsonls_parallel_collect_with<A, F>(
        &self,
        inputs: Vec<PathBuf>,
        shards_dir: &Path,
        make_agg: F,
    ) -> Result<(A, AggregateBuildReport)>
    where
        A: Aggregator,
        F: Fn() -> A + Send + Sync,
    {
        self.aggregate_jsonls_parallel_collect_with_policy(
            inputs,
            shards_dir,
            make_agg,
            AggregatePartialReadPolicy::Strict,
        )
    }

    /// Like [`Self::aggregate_jsonls_parallel_collect_with`], but lets library
    /// callers opt into merging partial-read shards.
    pub fn aggregate_jsonls_parallel_collect_with_policy<A, F>(
        &self,
        inputs: Vec<PathBuf>,
        shards_dir: &Path,
        make_agg: F,
        partial_policy: AggregatePartialReadPolicy,
    ) -> Result<(A, AggregateBuildReport)>
    where
        A: Aggregator,
        F: Fn() -> A + Send + Sync,
    {
        create_dir_all_with_backoff(shards_dir, 16, 50)
            .with_context(|| format!("creating shards_dir {}", shards_dir.display()))?;
        let run_token = aggregate_run_token();
        let run_dir = aggregate_run_dir(shards_dir, &run_token);
        create_dir_all_with_backoff(&run_dir, 16, 50)
            .with_context(|| format!("creating aggregate run directory {}", run_dir.display()))?;
        let staging_dir = ensure_staging_dir(&run_dir)?;
        let shard_paths = shard_names_for_inputs(&run_dir, &run_token, &inputs);

        with_thread_pool(self.opts.parallelism, || {
            let BuildOutcome { shards, report } = build_aggregate_shards_with::<A, F>(
                &inputs,
                &shard_paths,
                &staging_dir,
                self.opts.progress,
                &make_agg,
                partial_policy,
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

            Ok((total, report))
        })
    }
}
