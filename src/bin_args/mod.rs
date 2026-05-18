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
    use clap::{CommandFactory, Parser};
    use retl::YearMonth;

    /// Render `<args> --help` for the configured `Cli` into a String. Avoids
    /// spawning the binary just to grep for substrings in --help output.
    fn render_help(args: &[&str]) -> String {
        let mut cmd = Cli::command();
        // Walk into nested subcommands so e.g. ["scan"] renders scan's help.
        let mut current = &mut cmd;
        for arg in args {
            current = current
                .find_subcommand_mut(arg)
                .unwrap_or_else(|| panic!("subcommand not found: {arg}"));
        }
        current.render_help().to_string()
    }

    /// Run `Cli::try_parse_from` with leading "retl" and assert that it fails.
    /// Returns the rendered error string so callers can assert on its content.
    fn parse_failure(argv: &[&str]) -> String {
        let mut v = vec!["retl"];
        v.extend_from_slice(argv);
        let err = Cli::try_parse_from(&v).expect_err("expected clap parse failure");
        err.to_string()
    }

    // -----------------------------------------------------------------------
    // Snapshot-style guards on the public CLI surface. Migrated from the
    // former tests/cli_help.rs (which spawned the `retl` binary 18 times to
    // do exactly this) into in-process clap help rendering — zero process
    // spawn cost, same coverage.
    // -----------------------------------------------------------------------

    #[test]
    fn root_help_lists_all_subcommands() {
        let h = render_help(&[]);
        for s in [
            "describe",
            "scan",
            "dedupe",
            "export",
            "quickstart",
            "convert",
            "count",
            "integrity",
            "aggregate",
            "parents",
            "first-seen",
        ] {
            assert!(h.contains(s), "root --help should mention `{s}`:\n{h}");
        }
    }

    #[test]
    fn describe_help_advertises_discovery_flags() {
        let h = render_help(&["describe"]);
        for s in ["--data-dir", "--source", "--start", "--end"] {
            assert!(h.contains(s), "describe --help missing `{s}`:\n{h}");
        }
    }

    #[test]
    fn quickstart_help_advertises_out_dir() {
        let h = render_help(&["quickstart"]);
        assert!(h.contains("--out-dir"));
    }

    #[test]
    fn parents_help_advertises_required_flags() {
        let h = render_help(&["parents"]);
        for s in [
            "--spool",
            "--ids-file",
            "--parent-id",
            "--id-kind",
            "--cache",
            "--out",
            "--start",
            "--end",
            "--window-months",
            "--parent-fields",
            "--parent-full",
            "--resume",
            "--inflight-bytes",
            "--inflight-groups",
        ] {
            assert!(h.contains(s), "parents --help missing `{s}`:\n{h}");
        }
    }

    #[test]
    fn first_seen_help_advertises_out() {
        let h = render_help(&["first-seen"]);
        assert!(h.contains("--out"));
        assert!(h.contains("--resume"));
    }

    #[test]
    fn export_help_advertises_export_only_flags() {
        let h = render_help(&["export"]);
        for s in [
            "--zst-level",
            "--resume",
            "--whitelist",
            "--strict-whitelist",
            "--human-timestamps",
            "--inflight-bytes",
            "--inflight-groups",
        ] {
            assert!(h.contains(s), "export --help missing `{s}`:\n{h}");
        }
    }

    #[test]
    fn dedupe_help_advertises_key_out_and_inflight() {
        let h = render_help(&["dedupe"]);
        for s in [
            "--key",
            "--out",
            "--inflight-bytes",
            "--inflight-groups",
            "--strict-key",
            "--resume",
            "json:/pointer",
        ] {
            assert!(h.contains(s), "dedupe --help missing `{s}`:\n{h}");
        }
    }

    #[test]
    fn scan_help_advertises_common_flags() {
        let h = render_help(&["scan"]);
        for s in [
            "--data-dir",
            "--work-dir",
            "--start",
            "--end",
            "--parallelism",
            "--file-concurrency",
            "--no-progress",
            "--source",
            "--subreddit",
            "--id",
            "--ids-file",
            "--author",
            "--exclude-author",
            "--exclude-common-bots",
            "--author-regex",
            "--keyword",
            "--keyword-all",
            "--exclude-keyword",
            "--text-regex",
            "--min-score",
            "--max-score",
            "--after",
            "--before",
            "inclusive",
            "exclusive",
            "--contains-url",
            "--no-url",
            "--domain",
            "--json",
            "--include-deleted",
            "--resume",
        ] {
            assert!(h.contains(s), "scan --help missing `{s}`:\n{h}");
        }
        for s in ["--whitelist", "--strict-whitelist", "--human-timestamps"] {
            assert!(!h.contains(s), "scan --help should NOT mention `{s}`:\n{h}");
        }
    }

    #[test]
    fn export_help_advertises_format_and_out() {
        let h = render_help(&["export"]);
        for s in [
            "--format",
            "jsonl",
            "json",
            "spool",
            "zst",
            "partitioned-jsonl",
            "--out",
            "--pretty",
            "Field-indent the JSON array",
        ] {
            assert!(h.contains(s), "export --help missing `{s}`:\n{h}");
        }
    }

    #[test]
    fn convert_help_advertises_fields_spool_format_and_out() {
        let h = render_help(&["convert"]);
        for s in ["--field", "--spool", "--format", "csv", "tsv", "--out"] {
            assert!(h.contains(s), "convert --help missing `{s}`:\n{h}");
        }
    }

    #[test]
    fn count_help_advertises_modes() {
        let h = render_help(&["count"]);
        for s in ["--mode", "month", "author", "--resume"] {
            assert!(h.contains(s), "count --help missing `{s}`:\n{h}");
        }
    }

    #[test]
    fn integrity_help_advertises_modes_and_sample() {
        let h = render_help(&["integrity"]);
        for s in ["--mode", "quick", "full", "--sample-bytes", "--collect"] {
            assert!(h.contains(s), "integrity --help missing `{s}`:\n{h}");
        }
    }

    #[test]
    fn aggregate_help_advertises_spool_inputs_out_and_runtime_flags_only() {
        let h = render_help(&["aggregate"]);
        for s in [
            "INPUTS",
            "--spool",
            "--out",
            "--by",
            "--metric",
            "--top",
            "--scientific",
            "Field-indent the final JSON",
            "--parallelism",
            "--no-progress",
            "--shards-dir",
        ] {
            assert!(h.contains(s), "aggregate --help missing `{s}`:\n{h}");
        }
        for s in [
            "--data-dir",
            "--work-dir",
            "--start",
            "--end",
            "--source",
            "--subreddit",
            "--include-deleted",
            "--exclude-common-bots",
            "--file-concurrency",
        ] {
            assert!(
                !h.contains(s),
                "aggregate --help should NOT mention `{s}`:\n{h}"
            );
        }
    }

    #[test]
    fn version_flag_works() {
        // clap returns DisplayVersion as an "error" that carries the version
        // string; that's still a non-spawn parse success in clap's model.
        let err = Cli::try_parse_from(["retl", "--version"]).expect_err("--version uses err path");
        assert!(matches!(err.kind(), clap::error::ErrorKind::DisplayVersion));
    }

    #[test]
    fn unknown_subcommand_fails_clean() {
        let msg = parse_failure(&["not-a-real-subcommand"]);
        assert!(
            msg.to_lowercase().contains("error") || msg.contains("unrecognized"),
            "expected error wording, got: {msg}"
        );
    }

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
