use anyhow::{Context, Result};
use retl::{
    convert_jsonl_to_csv, convert_jsonl_to_tsv, discover_sources_checked,
    discover_upstream_manifests_from_inputs, file_identities, file_identity, for_each_line_cfg,
    format_year_month_ranges, missing_month_diagnostics, path_to_stable_string, plan_files,
    plan_files_checked, read_line_capped, total_compressed_size,
    upstream_manifest_for_directory, write_run_manifest, write_text_atomic,
    AggregateBuildReport, CorpusAvailability, CorpusLocalStatus, CorpusManifest, CorpusPlanItem,
    CorpusSnapshot, ExportFormat, FileIdentity, FileKind, IntegrityMode, KeyExtractor,
    ManifestDestination, ParentIds, ParentPayloadSpec, RedditETL, RunManifestInput,
    RunManifestStart, Sources, TabularExportOptions, YearMonth, DEFAULT_MAX_LINE_BYTES,
    MAX_RETAINED_FAILURES,
};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
#[cfg(test)]
use std::fs;
use std::io::{self, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::bin_args::{
    AggregateArgs, AggregateFmt, CommonOpts, ConvertArgs, ConvertFmt, CorpusArgs, CorpusCommand,
    CorpusManifestArgs, CorpusPlanArgs, CorpusPlanFmt, CountArgs, CountMode, DedupeArgs,
    DescribeArgs, ExportArgs, ExportFmt, FirstSeenArgs, IntegrityArgs, IntegrityModeArg,
    ParentIdKindArg, ParentsArgs, QueryOpts, QuickstartArgs, SampleArgs, ScanArgs, SchemaArgs,
    SchemaFmt, SourceArg,
};
use crate::bin_helpers::{
    build_etl, discover_spool_parts, emit_partial_read_report, plan, stream_extract_to_stdout,
    stream_path_output_to_stdout, GroupBySpec, GroupMetricAgg, MetricSpec, RecCount,
};

const CLI_TEXT_WRITE_BUF_BYTES: usize = 64 * 1024;
const QUICK_SAMPLE_WARN_BELOW_BYTES: u64 = 4096;

/// Outcome of a subcommand handler that influences the process exit code.
///
/// Most handlers just return `Result<()>` (mapped to exit 0 on `Ok`, 1 on
/// `Err`). `integrity` returns [`HandlerOutcome::CorruptFilesFound`] for the
/// 'corruption detected' result so `main` can exit 2 *after* the monitor
/// finalizes. A bare `std::process::exit(2)` from the handler runs no
/// destructors, so the `MonitorHandle` would never emit the terminal
/// `run.summary` nor mark the `--status-file` finished — a watcher would
/// then misread a normal 'corruption found' result as an abrupt hard-kill.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HandlerOutcome {
    /// Handler finished normally; `main` exits 0.
    Done,
    /// `integrity` found at least one corrupt corpus file; `main` exits 2.
    CorruptFilesFound,
}
const QUICKSTART_SAMPLE_JSONL: &str = include_str!("../../benches/data/sample.jsonl");
const QUICKSTART_ZST_LEVEL: i32 = 3;
