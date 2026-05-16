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
mod tests {
    use super::*;
    use clap::Parser;
    use retl::YearMonth;

    #[test]
    fn cli_accepts_huge_resource_flags_for_builder_clamp() {
        let huge = usize::MAX.to_string();
        let cli = Cli::try_parse_from([
            "retl",
            "scan",
            "--parallelism",
            huge.as_str(),
            "--file-concurrency",
            huge.as_str(),
        ])
        .expect("resource flag parsing should succeed; builders clamp later");

        match cli.command {
            Command::Scan(args) => {
                assert_eq!(args.common.parallelism, Some(usize::MAX));
                assert_eq!(args.common.file_concurrency, Some(usize::MAX));
            }
            other => panic!("expected scan command, got {other:?}"),
        }
    }

    #[test]
    fn cli_accepts_corpus_plan_command() {
        let cli = Cli::try_parse_from([
            "retl", "corpus", "plan", "--source", "rc", "--start", "2006-01", "--end", "2006-02",
        ])
        .expect("corpus plan should parse");

        match cli.command {
            Command::Corpus(CorpusArgs {
                command: CorpusCommand::Plan(args),
            }) => {
                assert_eq!(args.source.label(), "rc");
                assert_eq!(args.start, YearMonth::new(2006, 1));
                assert_eq!(args.end, YearMonth::new(2006, 2));
            }
            other => panic!("expected corpus plan command, got {other:?}"),
        }
    }

    #[test]
    fn timestamp_aliases_parse_into_query_opts() {
        let cli = Cli::try_parse_from([
            "retl",
            "scan",
            "--start-time",
            "2020-11-30T12:00Z",
            "--end-time",
            "2020-12-01T12:00Z",
        ])
        .expect("timestamp aliases should parse");

        match cli.command {
            Command::Scan(args) => {
                assert_eq!(args.query.after, Some(1_606_737_600));
                assert_eq!(args.query.before, Some(1_606_824_000));
            }
            other => panic!("expected scan command, got {other:?}"),
        }
    }
}
