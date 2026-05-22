use crate::atomic_write::{ensure_staging_dir, write_at_path_atomic, write_jsonl_atomic};
use crate::ndjson::for_each_jsonl_line_cfg;
use crate::pipeline::RedditETL;
use crate::progress::make_count_progress;
use crate::run_manifest::{
    discover_upstream_manifests_from_inputs, file_identities, maybe_write_run_manifest,
    ManifestDestination, RunManifestInput, RunManifestStart,
};
use crate::util::with_thread_pool;
use anyhow::{Context, Result};
use indicatif::ProgressBar;
use rayon::prelude::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
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
    /// Total non-empty JSONL lines ingested across the [`merged_shards`]. Lines
    /// from fatal inputs and from partial inputs dropped under the strict
    /// partial-read policy are excluded — this counts only records that
    /// reached the merged aggregate.
    ///
    /// [`merged_shards`]: AggregateBuildReport::merged_shards
    pub records_ingested: u64,
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

    /// True when shards were merged but every one of them ingested zero
    /// records — a "successful" aggregate whose output is empty. This is
    /// almost always a wrong input path or an empty spool directory rather
    /// than intent, so callers should surface it as a warning.
    pub fn ingested_zero_records(&self) -> bool {
        self.merged_shards > 0 && self.records_ingested == 0
    }
}
