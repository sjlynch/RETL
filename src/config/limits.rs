use crate::date::YearMonth;
use crate::mem::AdaptiveMemCfg;
use crate::parents::ParentPayloadSpec;
use parking_lot::Mutex;
use serde::Serialize;
use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Structured error returned when ETL option builders contain invalid settings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConfigBuildError {
    InvalidDateRange { start: YearMonth, end: YearMonth },
}

impl fmt::Display for ConfigBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigBuildError::InvalidDateRange { start, end } => {
                write!(f, "invalid date range: start {start} is after end {end}")
            }
        }
    }
}

impl Error for ConfigBuildError {}

/// Maximum shard count for open-all-writers sharding layers.
///
/// `ShardedWriter`, `ShardedKVWriter`, parent-id shards, and bucketing shard
/// routers keep one `BufWriter<File>` open per shard. Capping at the historic
/// default of 256 bounds file-descriptor use and writer-buffer RAM while still
/// giving enough fan-out for large Reddit corpora.
pub const MAX_SHARDS: usize = 256;

/// Hard ceiling on scoped Rayon worker threads requested through RETL options.
///
/// The effective cap is [`max_parallelism_limit`], which is a conservative
/// multiple of `std::thread::available_parallelism()` and never exceeds this
/// hard maximum. Oversized builder/CLI values are clamped with a warning.
pub const MAX_RAYON_THREADS: usize = 256;

/// Maximum number of monthly zstd files RETL will decode concurrently.
///
/// Each in-flight Reddit zstd frame can reserve a multi-GiB decode window, so
/// `file_concurrency` is intentionally capped below the worker-thread limit.
pub const MAX_FILE_CONCURRENCY: usize = 8;

/// Effective upper bound for `.parallelism(n)` and `--parallelism`.
///
/// RETL allows a small oversubscription factor for I/O-heavy phases but keeps
/// typos such as `usize::MAX` far away from Rayon/OS thread limits.
pub fn max_parallelism_limit() -> usize {
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    cpus.saturating_mul(4).clamp(1, MAX_RAYON_THREADS)
}

pub(crate) fn clamp_shard_count(shards: usize, component: &str) -> usize {
    let clamped = shards.max(1).min(MAX_SHARDS);
    if shards > MAX_SHARDS {
        tracing::warn!(
            component,
            requested = shards,
            max = MAX_SHARDS,
            "clamping shard count to bound open file handles and writer buffers"
        );
    }
    clamped
}

pub(crate) fn clamp_parallelism_threads(threads: usize, component: &str) -> usize {
    let max = max_parallelism_limit();
    let clamped = threads.max(1).min(max);
    if threads > max {
        tracing::warn!(
            component,
            requested = threads,
            max,
            hard_max = MAX_RAYON_THREADS,
            "clamping Rayon worker count to a safe process-local limit"
        );
    }
    clamped
}

pub(crate) fn clamp_file_concurrency(n: usize, component: &str) -> usize {
    let clamped = n.max(1).min(MAX_FILE_CONCURRENCY);
    if n > MAX_FILE_CONCURRENCY {
        tracing::warn!(
            component,
            requested = n,
            max = MAX_FILE_CONCURRENCY,
            "clamping zstd file concurrency to bound decoder-window memory"
        );
    }
    clamped
}
