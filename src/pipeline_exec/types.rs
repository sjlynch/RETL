use crate::atomic_write::{
    ensure_staging_dir, sweep_stale_inprogress, write_jsonl_atomic, write_zst_atomic,
};
use crate::config::ETLOptions;
use crate::date::YearMonth;
use crate::dedupe::{
    build_runs_sorted_with_key_stats, merge_runs_sorted_with_key_stats, DedupeCfg,
};
use crate::filters::{
    bounds_tuple, matches_full, matches_minimal, resolve_target_subs_from, within_bounds,
    ym_from_epoch, DateBounds,
};
use crate::key_extractor::KeyExtractor;
use crate::kv_shard::ShardedKVWriter;
use crate::paths::{
    discover_sources_checked, log_missing_month_warnings, plan_files_checked, FileJob, FileKind,
};
use crate::pipeline::{RedditETL, ScanPlan};
use crate::progress::{make_progress_bar_labeled, total_compressed_size};
use crate::progress_manifest::{ManifestAccumulator, MonthEntry};
use crate::query::QuerySpec;
use crate::run_manifest::{
    corpus_snapshot_from_etl, etl_options_value, maybe_write_run_manifest, scan_query_value,
    ManifestDestination, RunManifestInput, RunManifestStart,
};
use crate::shard::{ShardedWriter, UsernameStream};
use crate::stitch::{concat_tsvs, stitch_tmp_parts, stitch_tmp_parts_to_json_array};
use crate::streaming::{
    claim_record_or_stop, is_record_limit_reached, process_file_for_usernames_with_skip,
    stream_job_with_partial_policy, RecordLimit, StreamJobResult, WhitelistMatchTracker,
};
use crate::util::{
    stable_fnv1a_hex, system_time_parts,
    with_thread_pool,
};
use crate::zstd_jsonl::{
    for_each_line_with_opts_status, malformed_json_error, parse_minimal, LineStreamOpts,
    MinimalRecord, PartialReadPolicy,
};
use anyhow::{anyhow, Context, Result};
use indicatif::ProgressBar;
use rayon::prelude::*;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::fs;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Export format for partitioned corpus-style outputs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExportFormat {
    Jsonl,
    Zst,
}

/// Options for [`ScanPlan::extract_to_csv`] and [`ScanPlan::extract_to_tsv`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TabularExportOptions {
    /// Emit a header row containing the requested field names.
    pub header: bool,
}

impl Default for TabularExportOptions {
    fn default() -> Self {
        Self { header: true }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TabularFormat {
    Csv,
    Tsv,
}

impl TabularFormat {
    fn label(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Tsv => "tsv",
        }
    }

    fn tmp_dir_name(self) -> &'static str {
        match self {
            Self::Csv => "extract_csv_q_tmp",
            Self::Tsv => "extract_tsv_q_tmp",
        }
    }

    fn row_suffix(self) -> &'static str {
        match self {
            Self::Csv => ".csvpart",
            Self::Tsv => ".tsvpart",
        }
    }
}

/// Summary returned by [`ScanPlan::dedupe_keys_to_lines_with_stats`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DedupeKeySummary {
    /// Records that matched the scan query before dedupe key extraction.
    pub matched_records: u64,
    /// Distinct keys written to the output.
    pub unique_keys: u64,
    /// Matched records for which [`KeyExtractor::key_from_line`] returned
    /// `Ok(None)` (missing, null, or otherwise non-extractable key). These
    /// records are omitted from the dedupe output.
    pub key_extractions_failed: u64,
}

impl DedupeKeySummary {
    /// Fraction of matched records omitted because no dedupe key was found.
    pub fn key_drop_rate(&self) -> f64 {
        if self.matched_records == 0 {
            0.0
        } else {
            self.key_extractions_failed as f64 / self.matched_records as f64
        }
    }
}

/// Finalization choice for extract operations (internal).
enum Finalize {
    Jsonl,
    JsonArray { pretty: bool },
}

const FILE_PREFIX_RC: &str = "part_RC";
const FILE_PREFIX_RS: &str = "part_RS";
const ANALYTICS_CHECKPOINT_OPERATION: &str = "analytics-matched-jsonl-v1";
const DEDUPE_KEY_DROP_WARN_RATE: f64 = 0.01;
static EXTRACT_SCRATCH_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
struct PartialScanError {
    path: PathBuf,
    written: u64,
}

impl fmt::Display for PartialScanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "zstd decode error while streaming {}; {} record(s) were decoded before the file was skipped",
            self.path.display(),
            self.written
        )
    }
}

impl std::error::Error for PartialScanError {}
