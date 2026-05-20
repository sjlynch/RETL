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
mod parents;
mod parsers;
mod quickstart;
mod sample;
mod scan;
mod schema;

pub(crate) use aggregate::AggregateArgs;
#[allow(unused_imports)]
pub(crate) use aggregate::AggregateRuntimeOpts;
pub(crate) use common::{CommonOpts, QueryOpts, SourceArg};
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
    /// Subcommands that flatten [`CommonOpts`] expose it here so the binary
    /// can read observability flags once, up front, before dispatch.
    /// Returns `None` for the analytics/manifest subcommands that have
    /// their own argument shapes (`aggregate`, `corpus`, `describe`,
    /// `parents`, `quickstart`, `convert`, `schema`).
    pub(crate) fn common_opts(&self) -> Option<&CommonOpts> {
        match self {
            Command::Scan(a) => Some(&a.common),
            Command::Export(a) => Some(&a.common),
            Command::Dedupe(a) => Some(&a.common),
            Command::Count(a) => Some(&a.common),
            Command::Sample(a) => Some(&a.common),
            Command::Integrity(a) => Some(&a.common),
            Command::FirstSeen(a) => Some(&a.common),
            _ => None,
        }
    }
}

#[derive(Subcommand, Debug)]
pub(crate) enum Command {
    /// Inspect discovered corpus months, file counts, and compressed bytes without decoding.
    #[command(alias = "ls", alias = "plan")]
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
    /// Export filtered records as JSONL, JSON, spool files, or partitioned corpus files.
    Export(ExportArgs),
    /// Flatten existing JSONL/spool files into CSV or TSV columns.
    Convert(ConvertArgs),
    /// Count records by month, or write per-author counts to TSV.
    Count(CountArgs),
    /// Validate `.zst` monthly files (quick sample or full decode).
    ///
    /// `integrity` only reads and checks corpus files; it emits no records and
    /// writes no provenance manifest, so the shared `--no-manifest` flag has no
    /// effect here and passing it logs a warning.
    Integrity(IntegrityArgs),
    /// Aggregate JSONL inputs into JSON record counts or built-in TSV rollups.
    Aggregate(AggregateArgs),
    /// Resolve parent comments/submissions from a spool directory or direct IDs.
    Parents(ParentsArgs),
    /// Build a per-author "first-seen" timestamp index TSV.
    #[command(name = "first-seen")]
    FirstSeen(FirstSeenArgs),
}

#[cfg(test)]
mod tests;
