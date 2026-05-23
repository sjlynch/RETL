//! CLI argument structs for the `retl` binary.
//!
//! `Cli` / `Command` describe the subcommand surface; `CommonOpts` plus the
//! per-subcommand `*Args` structs back the clap derive. These types are
//! binary-only — keep them out of `src/lib.rs`.

use clap::{Parser, Subcommand};

mod aggregate;
mod common;
mod convert;
mod corpus;
mod count;
mod dedupe;
mod describe;
mod export;
mod first_seen;
mod integrity;
mod load;
mod parents;
mod parsers;
mod quickstart;
mod sample;
mod scan;
mod schema;

pub(crate) use aggregate::{AggregateArgs, AggregateFmt};
#[allow(unused_imports)]
pub(crate) use aggregate::AggregateRuntimeOpts;
pub(crate) use common::{CommonOpts, MonitorOpts, QueryOpts, SourceArg};
pub(crate) use convert::{ConvertArgs, ConvertFmt};
pub(crate) use corpus::{
    CorpusArgs, CorpusCommand, CorpusManifestArgs, CorpusPlanArgs, CorpusPlanFmt,
};
pub(crate) use count::{CountArgs, CountMode};
pub(crate) use dedupe::DedupeArgs;
pub(crate) use describe::DescribeArgs;
pub(crate) use export::{ExportArgs, ExportFmt};
pub(crate) use first_seen::FirstSeenArgs;
pub(crate) use integrity::{IntegrityArgs, IntegrityModeArg};
pub(crate) use load::LoadArgs;
// `LoadMode` and `IfExists` are referenced only inside the gated `run_load`
// handler and the in-process clap parse tests.
#[cfg_attr(
    not(any(test, feature = "duckdb-load")),
    allow(unused_imports)
)]
pub(crate) use load::{IfExists, LoadMode};
pub(crate) use parents::{ParentIdKindArg, ParentsArgs};
pub(crate) use quickstart::QuickstartArgs;
pub(crate) use sample::SampleArgs;
pub(crate) use scan::ScanArgs;
pub(crate) use schema::{SchemaArgs, SchemaFmt};

#[derive(Parser, Debug)]
#[command(
    name = "retl",
    version,
    about = "Reddit ETL toolkit — inspect, scan, export, count, validate, and aggregate Reddit RC/RS .zst dumps.",
    long_about = None,
)]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Command,
}

impl Command {
    /// Expose each subcommand's [`MonitorOpts`] so the binary can install
    /// the monitor once, up front, before dispatch. Most commands carry it
    /// inside their flattened [`CommonOpts`]; `parents` and `aggregate`
    /// flatten [`MonitorOpts`] directly because they have their own
    /// argument shape. Returns `None` only for the short-running
    /// analytics/manifest subcommands (`corpus`, `describe`, `quickstart`,
    /// `convert`, `schema`), which get default (no-op) monitoring.
    pub(crate) fn monitor_opts(&self) -> Option<&MonitorOpts> {
        match self {
            Command::Scan(a) => Some(&a.common.monitor),
            Command::Export(a) => Some(&a.common.monitor),
            Command::Dedupe(a) => Some(&a.common.monitor),
            Command::Count(a) => Some(&a.common.monitor),
            Command::Sample(a) => Some(&a.common.monitor),
            Command::Integrity(a) => Some(&a.common.monitor),
            Command::FirstSeen(a) => Some(&a.common.monitor),
            Command::Parents(a) => Some(&a.monitor),
            Command::Aggregate(a) => Some(&a.monitor),
            _ => None,
        }
    }
}

#[derive(Subcommand, Debug)]
pub(crate) enum Command {
    /// Inspect discovered corpus months, file counts, and compressed bytes without decoding.
    ///
    /// Aliased as `retl ls`. The former `retl plan` alias was dropped because
    /// it collided with `retl corpus plan` (the acquisition checklist).
    #[command(alias = "ls")]
    Describe(DescribeArgs),
    /// Plan corpus acquisition from a versioned manifest.
    Corpus(CorpusArgs),
    /// Discover top-level JSON fields and their common types from sampled records.
    Schema(SchemaArgs),
    /// Print a small sample of matching records (defaults to 10 JSONL records on stdout).
    #[command(alias = "preview", alias = "head")]
    Sample(SampleArgs),
    /// Generate and scan a built-in tiny corpus so new installs can verify RETL without Reddit dumps.
    #[command(alias = "demo")]
    Quickstart(QuickstartArgs),
    /// Scan and emit unique usernames matching the query selection.
    Scan(ScanArgs),
    /// Emit distinct keys (author, subreddit, or JSON pointer) matching the query selection.
    #[command(alias = "unique", alias = "distinct")]
    Dedupe(DedupeArgs),
    /// Export filtered records as JSONL, JSON, CSV/TSV, spool, partitioned
    /// JSONL/ZST/Parquet trees, or a single Parquet file (parquet requires the
    /// `parquet` cargo feature).
    Export(ExportArgs),
    /// Flatten existing JSONL/spool files into CSV or TSV columns.
    Convert(ConvertArgs),
    /// Count records by month, or write per-author counts to TSV.
    Count(CountArgs),
    /// Validate `.zst` monthly files (quick sample or full decode).
    ///
    /// `integrity` only reads and checks whole corpus files; it never builds a
    /// record-level scan plan and writes no provenance manifest. The shared
    /// record filters `--subreddit`/`--include-deleted` are therefore rejected,
    /// and the inert `--no-manifest`/`--allow-partial` toggles log a warning.
    Integrity(IntegrityArgs),
    /// Aggregate JSONL inputs into JSON record counts, TSV rollups, or
    /// Parquet rollups (parquet requires the `parquet` cargo feature).
    Aggregate(AggregateArgs),
    /// Resolve parent comments/submissions from a spool directory or direct IDs.
    Parents(ParentsArgs),
    /// Build a per-author "first-seen" timestamp index TSV.
    #[command(name = "first-seen")]
    FirstSeen(FirstSeenArgs),
    /// Register Parquet output as a queryable DuckDB table or view.
    ///
    /// Available only when `retl` is built with the `duckdb-load` cargo
    /// feature (the bundled libduckdb adds ~10s to clean builds, so it is
    /// off by default). Without the feature the subcommand still parses but
    /// the handler errors out with a rebuild hint.
    Load(LoadArgs),
}

#[cfg(test)]
mod tests;
