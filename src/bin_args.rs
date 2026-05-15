//! CLI argument structs for the `retl` binary.
//!
//! `Cli` / `Command` describe the subcommand surface; `CommonOpts` plus the
//! per-subcommand `*Args` structs back the clap derive. These types are
//! binary-only — keep them out of `src/lib.rs`.

use clap::{Args, Parser, Subcommand, ValueEnum};
use retl::{Sources, YearMonth};
use std::path::PathBuf;

const SAMPLE_BYTES_ZERO_ERROR: &str =
    "--sample-bytes must be > 0; use --mode full for complete validation";

fn parse_positive_sample_bytes(raw: &str) -> Result<u64, String> {
    let bytes = raw
        .parse::<u64>()
        .map_err(|e| format!("invalid byte count: {e}"))?;
    if bytes == 0 {
        Err(SAMPLE_BYTES_ZERO_ERROR.to_string())
    } else {
        Ok(bytes)
    }
}

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
    /// Scan and emit unique usernames matching the query selection.
    Scan(ScanArgs),
    /// Emit distinct keys (author, subreddit, or JSON pointer) matching the query selection.
    #[command(alias = "unique", alias = "distinct")]
    Dedupe(DedupeArgs),
    /// Export filtered records as JSONL, JSON, spool files, or partitioned corpus files.
    Export(ExportArgs),
    /// Count records by month, or write per-author counts to TSV.
    Count(CountArgs),
    /// Validate `.zst` monthly files (quick sample or full decode).
    Integrity(IntegrityArgs),
    /// Aggregate JSONL inputs into JSON record counts or built-in TSV rollups.
    Aggregate(AggregateArgs),
    /// Resolve and attach parent comments/submissions onto a spool directory.
    Parents(ParentsArgs),
    /// Build a per-author "first-seen" timestamp index TSV.
    #[command(name = "first-seen")]
    FirstSeen(FirstSeenArgs),
}

// -----------------------------------------------------------------------------
// Common flags shared by corpus-scanning subcommands.
// -----------------------------------------------------------------------------

#[derive(Args, Debug, Clone)]
pub(crate) struct CommonOpts {
    /// Path to corpus base dir (containing `comments/` and `submissions/`).
    #[arg(long, default_value = "./data")]
    pub(crate) data_dir: PathBuf,

    /// Scratch directory for sharded writers and stitched intermediates.
    #[arg(long, default_value = "./etl_work")]
    pub(crate) work_dir: PathBuf,

    /// Inclusive start month (YYYY-MM).
    #[arg(long, value_name = "YYYY-MM")]
    pub(crate) start: Option<YearMonth>,

    /// Inclusive end month (YYYY-MM).
    #[arg(long, value_name = "YYYY-MM")]
    pub(crate) end: Option<YearMonth>,

    /// Number of Rayon worker threads (defaults to the global pool; clamped to RETL's safe cap).
    #[arg(long)]
    pub(crate) parallelism: Option<usize>,

    /// Number of monthly files processed concurrently (clamped to protect zstd decoder memory).
    #[arg(long)]
    pub(crate) file_concurrency: Option<usize>,

    /// Disable progress bars.
    #[arg(long)]
    pub(crate) no_progress: bool,

    /// Source selection: rc (comments), rs (submissions), or both.
    #[arg(long, value_enum, default_value_t = SourceArg::Both)]
    pub(crate) source: SourceArg,

    /// Subreddit name (repeat for multiple). If none given, all subreddits match.
    #[arg(long = "subreddit", short = 's')]
    pub(crate) subreddits: Vec<String>,

    /// Include pseudo-users that are excluded by default: [deleted], [removed], and empty authors.
    #[arg(long = "include-deleted", alias = "include-pseudo-users")]
    pub(crate) include_deleted: bool,

    /// Allow corrupt/truncated zstd monthly files to be skipped instead of
    /// failing the scan/export. Skipped paths are reported as JSON on stderr;
    /// resumable exports leave those months uncommitted so a later run retries.
    #[arg(long)]
    pub(crate) allow_partial: bool,
}

#[derive(Args, Debug, Clone)]
pub(crate) struct QueryOpts {
    /// Author allow-list entry (repeatable). `--author-in` is an alias.
    #[arg(long = "author", visible_alias = "author-in", value_name = "NAME")]
    pub(crate) authors: Vec<String>,

    /// Author deny-list entry (repeatable).
    #[arg(long = "exclude-author", value_name = "NAME")]
    pub(crate) exclude_authors: Vec<String>,

    /// Exclude RETL's default bot/service-account list, plus ETL_EXCLUDE_AUTHORS* augments.
    #[arg(long = "exclude-common-bots")]
    pub(crate) exclude_common_bots: bool,

    /// Regex matched against author names.
    #[arg(long = "author-regex", value_name = "REGEX")]
    pub(crate) author_regex: Option<String>,

    /// Keyword/text filter matched in comment bodies and submission titles/selftext.
    #[arg(long = "keyword", value_name = "TEXT")]
    pub(crate) keywords: Vec<String>,

    /// Minimum score (inclusive).
    #[arg(long = "min-score", value_name = "N")]
    pub(crate) min_score: Option<i64>,

    /// Maximum score (inclusive).
    #[arg(long = "max-score", value_name = "N")]
    pub(crate) max_score: Option<i64>,

    /// Keep only records that contain an http(s) URL in text or submission URL.
    #[arg(long = "contains-url")]
    pub(crate) contains_url: bool,

    /// Submission domain allow-list entry (repeatable). Comments have no domain and are dropped.
    #[arg(long = "domain", value_name = "DOMAIN")]
    pub(crate) domains: Vec<String>,

    /// Full-record JSON Pointer predicate. Repeatable. Syntax: `exists:/path`,
    /// `/path=value`, `/path!=value`, `/path>10`, `/path>=10`, `/path<10`,
    /// `/path<=10`, or `/path~=REGEX`. Values use JSON scalars when possible
    /// (`true`, `false`, `null`, numbers, or quoted strings); otherwise they
    /// are treated as strings.
    #[arg(long = "json", value_name = "PREDICATE")]
    pub(crate) json_predicates: Vec<String>,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
pub(crate) enum SourceArg {
    Rc,
    Rs,
    Both,
}

impl From<SourceArg> for Sources {
    fn from(s: SourceArg) -> Self {
        match s {
            SourceArg::Rc => Sources::Comments,
            SourceArg::Rs => Sources::Submissions,
            SourceArg::Both => Sources::Both,
        }
    }
}

// -----------------------------------------------------------------------------
// Subcommand argument structs.
// -----------------------------------------------------------------------------

#[derive(Args, Debug)]
pub(crate) struct DescribeArgs {
    /// Path to corpus base dir (containing `comments/` and `submissions/`).
    #[arg(long, default_value = "./data")]
    pub(crate) data_dir: PathBuf,

    /// Inclusive start month (YYYY-MM).
    #[arg(long, value_name = "YYYY-MM")]
    pub(crate) start: Option<YearMonth>,

    /// Inclusive end month (YYYY-MM).
    #[arg(long, value_name = "YYYY-MM")]
    pub(crate) end: Option<YearMonth>,

    /// Source selection: rc (comments), rs (submissions), or both.
    #[arg(long, value_enum, default_value_t = SourceArg::Both)]
    pub(crate) source: SourceArg,
}

#[derive(Args, Debug)]
pub(crate) struct ScanArgs {
    #[command(flatten)]
    pub(crate) common: CommonOpts,
    #[command(flatten)]
    pub(crate) query: QueryOpts,
    /// Output file for usernames (default: stdout).
    #[arg(long, short)]
    pub(crate) out: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub(crate) struct DedupeArgs {
    #[command(flatten)]
    pub(crate) common: CommonOpts,
    #[command(flatten)]
    pub(crate) query: QueryOpts,
    /// Key to deduplicate: `author`, `subreddit`, or `json:/pointer`.
    /// JSON pointers should reference a scalar; strings, numbers, and bools are emitted as text.
    #[arg(long, value_name = "KEY")]
    pub(crate) key: String,
    /// Output text file, one unique key per line. Use `-` for stdout.
    #[arg(long, short)]
    pub(crate) out: PathBuf,
    /// Per-flush byte budget for bucketing/dedupe (`per_flush_cap = N / 2`).
    /// 0 disables the explicit cap and falls back to memory-fraction sampling.
    /// NOTE: this is NOT the worst-case peak — see `--inflight-groups`.
    #[arg(long)]
    pub(crate) inflight_bytes: Option<usize>,
    /// Bucketing channel depth (default 8). Worst-case bucketing peak is
    /// `(1 + inflight_groups) * inflight_bytes / 2`, NOT `inflight_bytes`.
    /// Set to 1 to match the declared `inflight_bytes` budget exactly.
    #[arg(long)]
    pub(crate) inflight_groups: Option<usize>,
    /// Error if any matched record does not contain the requested dedupe key.
    #[arg(long)]
    pub(crate) strict_key: bool,
}

#[derive(Args, Debug)]
pub(crate) struct ExportArgs {
    #[command(flatten)]
    pub(crate) common: CommonOpts,
    #[command(flatten)]
    pub(crate) query: QueryOpts,
    /// Output format.
    #[arg(long, value_enum, default_value_t = ExportFmt::Jsonl)]
    pub(crate) format: ExportFmt,
    /// Output destination — file for `jsonl`/`json` (use `-` for stdout),
    /// directory for `spool`/`zst`/`partitioned-jsonl`.
    #[arg(long, short)]
    pub(crate) out: PathBuf,
    /// Field-indent the JSON array (only with `--format json`).
    #[arg(long)]
    pub(crate) pretty: bool,
    /// zstd compression level for `.zst` outputs. Clamped to 1..=22 by the library.
    #[arg(long)]
    pub(crate) zst_level: Option<i32>,
    /// Whitelist of top-level fields to keep on export. Comma-separated, repeatable.
    /// Comments use `body`/`parent_id`/`link_id`; submissions use `title`/`selftext`/`domain`.
    #[arg(long, value_delimiter = ',')]
    pub(crate) whitelist: Vec<String>,
    /// Error if `--whitelist` matches zero fields in the first sampled records.
    #[arg(long)]
    pub(crate) strict_whitelist: bool,
    /// Convert `created_utc` to RFC3339 strings on export.
    #[arg(long)]
    pub(crate) human_timestamps: bool,
    /// Per-flush byte budget for bucketing/dedupe (`per_flush_cap = N / 2`).
    /// 0 disables the explicit cap and falls back to memory-fraction sampling.
    /// NOTE: this is NOT the worst-case peak — see `--inflight-groups`.
    #[arg(long)]
    pub(crate) inflight_bytes: Option<usize>,
    /// Bucketing channel depth (default 8). Worst-case bucketing peak is
    /// `(1 + inflight_groups) * inflight_bytes / 2`, NOT `inflight_bytes`.
    /// Set to 1 to match the declared `inflight_bytes` budget exactly.
    #[arg(long)]
    pub(crate) inflight_groups: Option<usize>,
    /// Resume a prior export with the same query/config/corpus. `jsonl`/`json`
    /// reuse per-month `.part_*.jsonl` files and `_progress.json` in a namespaced
    /// scratch dir under `--work-dir`; `spool`, `zst`, and `partitioned-jsonl`
    /// use `_progress.json` under `--out`. Changing filters, corpus paths,
    /// sources, date range, whitelist, timestamp formatting, or (for ZST)
    /// zst level invalidates the checkpoint and rebuilds the parts.
    #[arg(long)]
    pub(crate) resume: bool,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
pub(crate) enum ExportFmt {
    /// Single stitched `.jsonl` file (one record per line).
    Jsonl,
    /// Single `.json` file containing a JSON array of records.
    Json,
    /// Per-source per-month `part_RC_YYYY-MM.jsonl` / `part_RS_YYYY-MM.jsonl`.
    Spool,
    /// Corpus-style partitioned `.zst` files under `comments/` and `submissions/`.
    Zst,
    /// Corpus-style partitioned `.jsonl` files under `comments/` and `submissions/`.
    PartitionedJsonl,
}

#[derive(Args, Debug)]
pub(crate) struct CountArgs {
    #[command(flatten)]
    pub(crate) common: CommonOpts,
    #[command(flatten)]
    pub(crate) query: QueryOpts,
    /// Count mode: per month (`month`) or per author (`author`, writes TSV).
    #[arg(long, value_enum, default_value_t = CountMode::Month)]
    pub(crate) mode: CountMode,
    /// Output file (default stdout for `month`, required for `author`).
    /// Pass `-` to stream either mode to stdout.
    #[arg(long, short)]
    pub(crate) out: Option<PathBuf>,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
pub(crate) enum CountMode {
    Month,
    Author,
}

#[derive(Args, Debug)]
pub(crate) struct IntegrityArgs {
    #[command(flatten)]
    pub(crate) common: CommonOpts,
    /// Validation mode.
    #[arg(long, value_enum, default_value_t = IntegrityModeArg::Quick)]
    pub(crate) mode: IntegrityModeArg,
    /// Bytes (decompressed) to sample per file in quick mode. Must be positive;
    /// values below 4096 only validate a tiny prefix and emit a warning.
    #[arg(long, default_value_t = 64 * 1024, value_parser = parse_positive_sample_bytes)]
    pub(crate) sample_bytes: u64,
    /// Collect failures and print them only after all files finish.
    ///
    /// By default, integrity streams one `path<TAB>error` line to stdout as soon
    /// as each failure is discovered.
    #[arg(long)]
    pub(crate) collect: bool,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
pub(crate) enum IntegrityModeArg {
    Quick,
    Full,
}

#[derive(Args, Debug, Clone)]
pub(crate) struct AggregateRuntimeOpts {
    /// Number of Rayon worker threads (defaults to the global pool; clamped to RETL's safe cap).
    #[arg(long)]
    pub(crate) parallelism: Option<usize>,

    /// Disable progress bars.
    #[arg(long)]
    pub(crate) no_progress: bool,
}

#[derive(Args, Debug)]
pub(crate) struct AggregateArgs {
    #[command(flatten)]
    pub(crate) runtime: AggregateRuntimeOpts,
    /// Spool directory containing `part_RC_YYYY-MM.jsonl` /
    /// `part_RS_YYYY-MM.jsonl` files to aggregate.
    #[arg(long)]
    pub(crate) spool: Option<PathBuf>,
    /// JSONL input files to aggregate. Omit when using `--spool`.
    #[arg(num_args = 1.., value_name = "INPUTS")]
    pub(crate) inputs: Vec<PathBuf>,
    /// Output path. Without `--by`, writes JSON record-count state; with `--by`, writes TSV.
    #[arg(long, short)]
    pub(crate) out: PathBuf,
    /// Directory used for per-input aggregate shards (default: alongside `--out`).
    #[arg(long)]
    pub(crate) shards_dir: Option<PathBuf>,
    /// Field-indent the final JSON (only used when `--by` is omitted).
    #[arg(long)]
    pub(crate) pretty: bool,
    /// Built-in group key: `subreddit`, `month`, `author`, or `json:/pointer`.
    #[arg(long = "by")]
    pub(crate) by: Option<String>,
    /// Metric for grouped aggregation: `count` (default), `sum:/pointer`,
    /// `avg:/pointer`, `min:/pointer`, or `max:/pointer`.
    #[arg(long = "metric")]
    pub(crate) metric: Option<String>,
    /// Keep only the top N groups by metric value (ties sort by key).
    #[arg(long)]
    pub(crate) top: Option<usize>,
    /// Render grouped numeric TSV metrics with Rust's default f64 formatting
    /// (scientific notation when chosen by the formatter). By default RETL
    /// emits decimal strings for spreadsheet- and awk-friendly TSV.
    #[arg(long)]
    pub(crate) scientific: bool,
}

#[derive(Args, Debug)]
pub(crate) struct ParentsArgs {
    /// Spool directory containing `part_RC_YYYY-MM.jsonl` /
    /// `part_RS_YYYY-MM.jsonl` files produced by `retl export --format spool`.
    #[arg(long)]
    pub(crate) spool: PathBuf,
    /// Cache directory for resolved parent shards.
    #[arg(long)]
    pub(crate) cache: PathBuf,
    /// Output directory for spool files with `parent` payloads attached.
    #[arg(long, short)]
    pub(crate) out: PathBuf,
    /// Resume the parents pipeline by reusing cache shards and skipping
    /// already-attached output files only when their attach fingerprint still
    /// matches the spool file, parent cache, resolution window, and attach
    /// format. `export` is the other CLI subcommand that supports `--resume`;
    /// aggregate/count/scan/integrity/first-seen do not.
    #[arg(long)]
    pub(crate) resume: bool,
    /// Months of slack added on each side of the spool's date range when
    /// scanning the corpus to resolve parent payloads.
    #[arg(long, default_value_t = 3)]
    pub(crate) window_months: u32,
    /// Top-level parent fields to attach under `parent` (comma-separated,
    /// repeatable). Defaults to body,title,selftext for backwards-compatible
    /// output.
    #[arg(long = "parent-fields", value_delimiter = ',', value_name = "FIELD")]
    pub(crate) parent_fields: Vec<String>,
    /// Attach the full source parent JSON record under `parent` (plus RETL's
    /// `kind`/`id` metadata). Overrides `--parent-fields`.
    #[arg(long = "parent-full")]
    pub(crate) parent_full: bool,
    /// Path to corpus base dir (containing `comments/` and `submissions/`).
    #[arg(long, default_value = "./data")]
    pub(crate) data_dir: PathBuf,
    /// Scratch directory for sharded writers and stitched intermediates.
    #[arg(long, default_value = "./etl_work")]
    pub(crate) work_dir: PathBuf,
    /// Number of Rayon worker threads (defaults to the global pool; clamped to RETL's safe cap).
    #[arg(long)]
    pub(crate) parallelism: Option<usize>,
    /// Number of monthly files processed concurrently (clamped to protect zstd decoder memory).
    #[arg(long)]
    pub(crate) file_concurrency: Option<usize>,
    /// Disable progress bars.
    #[arg(long)]
    pub(crate) no_progress: bool,
    /// Per-flush byte budget for bucketing/dedupe (`per_flush_cap = N / 2`).
    /// NOTE: this is NOT the worst-case peak — see `--inflight-groups`.
    #[arg(long)]
    pub(crate) inflight_bytes: Option<usize>,
    /// Bucketing channel depth (default 8). Worst-case bucketing peak is
    /// `(1 + inflight_groups) * inflight_bytes / 2`, NOT `inflight_bytes`.
    /// Set to 1 to match the declared `inflight_bytes` budget exactly.
    #[arg(long)]
    pub(crate) inflight_groups: Option<usize>,
}

#[derive(Args, Debug)]
pub(crate) struct FirstSeenArgs {
    #[command(flatten)]
    pub(crate) common: CommonOpts,
    #[command(flatten)]
    pub(crate) query: QueryOpts,
    /// Output TSV file: `<author>\t<earliest_created_utc>` per line.
    #[arg(long, short)]
    pub(crate) out: PathBuf,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

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
}
