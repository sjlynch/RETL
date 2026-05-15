//! CLI argument structs for the `retl` binary.
//!
//! `Cli` / `Command` describe the subcommand surface; `CommonOpts` plus the
//! per-subcommand `*Args` structs back the clap derive. These types are
//! binary-only — keep them out of `src/lib.rs`.

use clap::{Args, Parser, Subcommand, ValueEnum};
use retl::{Sources, YearMonth};
use std::path::PathBuf;
use time::format_description::well_known::Rfc3339;
use time::{Date, Month, OffsetDateTime, Time, UtcOffset};

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

fn parse_timestamp_bound(raw: &str) -> Result<i64, String> {
    let s = raw.trim();
    if s.is_empty() {
        return Err("timestamp bound cannot be empty".to_string());
    }

    if looks_like_integer(s) {
        return s
            .parse::<i64>()
            .map_err(|e| format!("invalid Unix epoch seconds {s:?}: {e}"));
    }

    if let Ok(dt) = OffsetDateTime::parse(s, &Rfc3339) {
        return Ok(dt.unix_timestamp());
    }
    if let Some(with_seconds) = add_missing_rfc3339_seconds(s) {
        if let Ok(dt) = OffsetDateTime::parse(&with_seconds, &Rfc3339) {
            return Ok(dt.unix_timestamp());
        }
    }

    if let Some(ts) = parse_yyyy_mm_dd_utc(s)? {
        return Ok(ts);
    }

    Err(format!(
        "invalid timestamp {s:?}; expected Unix epoch seconds, RFC3339 timestamp (e.g. 2020-11-03T00:00:00Z), or YYYY-MM-DD date (UTC midnight)"
    ))
}

fn looks_like_integer(s: &str) -> bool {
    let digits = s
        .strip_prefix('+')
        .or_else(|| s.strip_prefix('-'))
        .filter(|rest| !rest.is_empty())
        .unwrap_or(s);
    !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
}

fn add_missing_rfc3339_seconds(s: &str) -> Option<String> {
    let t_idx = s.find('T').or_else(|| s.find('t'))?;
    let rest = &s[t_idx + 1..];
    let offset_rel = rest.find(|ch| matches!(ch, 'Z' | 'z' | '+' | '-'))?;
    let time_part = &rest[..offset_rel];
    let bytes = time_part.as_bytes();
    if !(bytes.len() == 5
        && bytes[2] == b':'
        && bytes[..2].iter().all(u8::is_ascii_digit)
        && bytes[3..].iter().all(u8::is_ascii_digit))
    {
        return None;
    }

    let offset_idx = t_idx + 1 + offset_rel;
    let mut out = String::with_capacity(s.len() + 3);
    out.push_str(&s[..t_idx]);
    out.push('T');
    out.push_str(&s[t_idx + 1..offset_idx]);
    out.push_str(":00");
    out.push_str(&s[offset_idx..]);
    if out.ends_with('z') {
        out.pop();
        out.push('Z');
    }
    Some(out)
}

fn parse_yyyy_mm_dd_utc(s: &str) -> Result<Option<i64>, String> {
    let bytes = s.as_bytes();
    if !(bytes.len() == 10 && bytes[4] == b'-' && bytes[7] == b'-') {
        return Ok(None);
    }
    if !(bytes[..4].iter().all(u8::is_ascii_digit)
        && bytes[5..7].iter().all(u8::is_ascii_digit)
        && bytes[8..].iter().all(u8::is_ascii_digit))
    {
        return Ok(None);
    }

    let year = s[..4]
        .parse::<i32>()
        .map_err(|e| format!("invalid date year in {s:?}: {e}"))?;
    let month_num = s[5..7]
        .parse::<u8>()
        .map_err(|e| format!("invalid date month in {s:?}: {e}"))?;
    let day = s[8..]
        .parse::<u8>()
        .map_err(|e| format!("invalid date day in {s:?}: {e}"))?;
    let month = Month::try_from(month_num)
        .map_err(|_| format!("invalid date {s:?}: month must be 01..12"))?;
    let date = Date::from_calendar_date(year, month, day)
        .map_err(|e| format!("invalid date {s:?}: {e}"))?;
    Ok(Some(
        OffsetDateTime::new_in_offset(date, Time::MIDNIGHT, UtcOffset::UTC).unix_timestamp(),
    ))
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
    /// Resolve and attach parent comments/submissions onto a spool directory.
    Parents(ParentsArgs),
    /// Build a per-author "first-seen" timestamp index TSV.
    #[command(name = "first-seen")]
    FirstSeen(FirstSeenArgs),
}

// -----------------------------------------------------------------------------
// Self-contained smoke/demo subcommands.
// -----------------------------------------------------------------------------

#[derive(Args, Debug, Clone)]
pub(crate) struct QuickstartArgs {
    /// Directory where the generated tiny corpus and scratch files are written.
    #[arg(long = "out-dir", default_value = "./retl_quickstart_sample")]
    pub(crate) out_dir: PathBuf,
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

    /// Do not write `<output>.retl-manifest.json` / `_retl_manifest.json` provenance sidecars.
    #[arg(long)]
    pub(crate) no_manifest: bool,

    /// Source selection: rc (comments), rs (submissions), or both.
    #[arg(long, value_enum, default_value_t = SourceArg::Both)]
    pub(crate) source: SourceArg,

    /// Subreddit name (repeat for multiple). If none given, all subreddits match. Blank values are rejected.
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
    /// Record ID allow-list entry (repeatable). Accepts bare IDs plus t1_/t3_ fullnames; prefixed IDs constrain comment/submission matching. Blank or duplicate IDs are rejected.
    #[arg(long = "id", value_name = "ID")]
    pub(crate) ids: Vec<String>,

    /// Newline-delimited record IDs to include. Blank lines and lines beginning with # are ignored; inline comments are not stripped. Repeatable.
    #[arg(long = "ids-file", value_name = "PATH")]
    pub(crate) ids_files: Vec<PathBuf>,

    /// Author allow-list entry (repeatable). `--author-in` is an alias. Blank values are rejected.
    #[arg(long = "author", visible_alias = "author-in", value_name = "NAME")]
    pub(crate) authors: Vec<String>,

    /// Author deny-list entry (repeatable). Blank values are rejected.
    #[arg(long = "exclude-author", value_name = "NAME")]
    pub(crate) exclude_authors: Vec<String>,

    /// Exclude RETL's default bot/service-account list, plus ETL_EXCLUDE_AUTHORS* augments.
    #[arg(long = "exclude-common-bots")]
    pub(crate) exclude_common_bots: bool,

    /// Regex matched against author names.
    #[arg(long = "author-regex", value_name = "REGEX")]
    pub(crate) author_regex: Option<String>,

    /// Keyword/text filter matched in comment bodies and submission titles/selftext (repeatable; any entry may match). Blank values are rejected.
    #[arg(long = "keyword", value_name = "TEXT")]
    pub(crate) keywords: Vec<String>,

    /// Require this keyword/text to be present across comment body or submission title/selftext (repeatable; all entries must match). Blank values are rejected.
    #[arg(long = "keyword-all", value_name = "TEXT")]
    pub(crate) keywords_all: Vec<String>,

    /// Reject records containing this keyword/text in comment body or submission title/selftext (repeatable). Blank values are rejected.
    #[arg(long = "exclude-keyword", value_name = "TEXT")]
    pub(crate) exclude_keywords: Vec<String>,

    /// Regex matched against comment body or submission title/selftext. Invalid or blank patterns are rejected before scanning.
    #[arg(long = "text-regex", value_name = "REGEX")]
    pub(crate) text_regex: Option<String>,

    /// Minimum score (inclusive).
    #[arg(long = "min-score", value_name = "N")]
    pub(crate) min_score: Option<i64>,

    /// Maximum score (inclusive).
    #[arg(long = "max-score", value_name = "N")]
    pub(crate) max_score: Option<i64>,

    /// Lower created_utc bound (inclusive). Accepts epoch seconds, RFC3339, or YYYY-MM-DD (UTC midnight).
    #[arg(long = "after", visible_alias = "start-time", value_name = "TIME", value_parser = parse_timestamp_bound)]
    pub(crate) after: Option<i64>,

    /// Upper created_utc bound (exclusive). Accepts epoch seconds, RFC3339, or YYYY-MM-DD (UTC midnight).
    #[arg(long = "before", visible_alias = "end-time", value_name = "TIME", value_parser = parse_timestamp_bound)]
    pub(crate) before: Option<i64>,

    /// Keep only records that contain an http(s) URL in text or an outbound link-submission URL.
    #[arg(long = "contains-url")]
    pub(crate) contains_url: bool,

    /// Keep only records without an http(s) URL in text and without an outbound link-submission URL.
    #[arg(long = "no-url")]
    pub(crate) no_url: bool,

    /// Submission domain allow-list entry (repeatable). Comments have no domain and are dropped. Blank values are rejected.
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

impl SourceArg {
    pub(crate) fn label(self) -> &'static str {
        match self {
            SourceArg::Rc => "rc",
            SourceArg::Rs => "rs",
            SourceArg::Both => "both",
        }
    }
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

    /// Decode a small sample from each selected month and report field schema.
    #[arg(long)]
    pub(crate) schema: bool,

    /// Records sampled per selected month when --schema is set.
    #[arg(long = "schema-sample", default_value_t = 100)]
    pub(crate) schema_sample: usize,

    /// Schema output format when --schema is set.
    #[arg(long = "schema-format", visible_alias = "format", value_enum, default_value_t = SchemaFmt::Tsv)]
    pub(crate) schema_format: SchemaFmt,

    /// Compare the requested --start/--end/--source against a corpus manifest.
    #[arg(long)]
    pub(crate) expected: bool,

    /// Custom corpus manifest JSON. Implies --expected; omitted means RETL's built-in manifest.
    #[arg(long = "manifest")]
    pub(crate) manifest: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub(crate) struct CorpusArgs {
    #[command(subcommand)]
    pub(crate) command: CorpusCommand,
}

#[derive(Subcommand, Debug)]
pub(crate) enum CorpusCommand {
    /// Emit a desired-vs-local download checklist for RC/RS monthly dumps.
    Plan(CorpusPlanArgs),
    /// Print RETL's built-in corpus manifest JSON.
    Manifest(CorpusManifestArgs),
}

#[derive(Args, Debug)]
pub(crate) struct CorpusPlanArgs {
    /// Destination corpus base dir that will contain comments/ and submissions/.
    #[arg(long, default_value = "./data")]
    pub(crate) dest: PathBuf,

    /// Inclusive start month (YYYY-MM).
    #[arg(long, value_name = "YYYY-MM")]
    pub(crate) start: YearMonth,

    /// Inclusive end month (YYYY-MM).
    #[arg(long, value_name = "YYYY-MM")]
    pub(crate) end: YearMonth,

    /// Source selection: rc (comments), rs (submissions), or both.
    #[arg(long, value_enum, default_value_t = SourceArg::Both)]
    pub(crate) source: SourceArg,

    /// Custom corpus manifest JSON. Omit to use RETL's built-in manifest.
    #[arg(long)]
    pub(crate) manifest: Option<PathBuf>,

    /// Output format.
    #[arg(long, value_enum, default_value_t = CorpusPlanFmt::Json)]
    pub(crate) format: CorpusPlanFmt,

    /// Output destination (default stdout). Use '-' for stdout.
    #[arg(long, short, default_value = "-")]
    pub(crate) out: PathBuf,

    /// Only emit available source/month rows whose expected file is missing locally.
    #[arg(long)]
    pub(crate) only_missing: bool,

    /// Compute SHA-256 for present files that have manifest checksums. This may read multi-GB files.
    #[arg(long)]
    pub(crate) verify_checksums: bool,
}

#[derive(Args, Debug)]
pub(crate) struct CorpusManifestArgs {
    /// Output destination (default stdout). Use '-' for stdout.
    #[arg(long, short, default_value = "-")]
    pub(crate) out: PathBuf,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
pub(crate) enum CorpusPlanFmt {
    Json,
    Tsv,
}

#[derive(Args, Debug, Clone)]
pub(crate) struct SchemaArgs {
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

    /// Records sampled per selected month.
    #[arg(long = "sample", default_value_t = 100)]
    pub(crate) sample_per_month: usize,

    /// Output format.
    #[arg(long, value_enum, default_value_t = SchemaFmt::Tsv)]
    pub(crate) format: SchemaFmt,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
pub(crate) enum SchemaFmt {
    Tsv,
    Json,
}

#[derive(Args, Debug)]
pub(crate) struct SampleArgs {
    #[command(flatten)]
    pub(crate) common: CommonOpts,
    #[command(flatten)]
    pub(crate) query: QueryOpts,
    /// Output format for the sample.
    #[arg(long, value_enum, default_value_t = ExportFmt::Jsonl)]
    pub(crate) format: ExportFmt,
    /// Output destination (default stdout). Directories are required for spool/zst/partitioned-jsonl.
    #[arg(long, short, default_value = "-")]
    pub(crate) out: PathBuf,
    /// Maximum matching records to emit. With file_concurrency >1, already-running workers may overshoot slightly.
    #[arg(long, visible_alias = "head", default_value_t = 10)]
    pub(crate) limit: u64,
    /// Field-indent the JSON array (only with `--format json`).
    #[arg(long)]
    pub(crate) pretty: bool,
    /// Whitelist of top-level fields to keep. Required for csv/tsv.
    #[arg(long, value_delimiter = ',')]
    pub(crate) whitelist: Vec<String>,
    /// Error if `--whitelist` matches zero fields in sampled records.
    #[arg(long)]
    pub(crate) strict_whitelist: bool,
    /// Convert `created_utc` to RFC3339 strings on JSON-family exports (not csv/tsv).
    #[arg(long)]
    pub(crate) human_timestamps: bool,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
pub(crate) enum ConvertFmt {
    /// RFC4180-style CSV with quoted multiline cells.
    Csv,
    /// Tab-separated text; tabs and line breaks in values are rejected.
    Tsv,
}

#[derive(Args, Debug)]
pub(crate) struct ConvertArgs {
    /// Scratch directory used when streaming converted output to stdout.
    #[arg(long, default_value = "./etl_work")]
    pub(crate) work_dir: PathBuf,
    /// RETL spool/parent-enriched directory containing part_RC_*.jsonl / part_RS_*.jsonl files.
    #[arg(long)]
    pub(crate) spool: Option<PathBuf>,
    /// Output format.
    #[arg(long, value_enum, default_value_t = ConvertFmt::Csv)]
    pub(crate) format: ConvertFmt,
    /// Output destination (use `-` for stdout).
    #[arg(long, short)]
    pub(crate) out: PathBuf,
    /// Column field selector. Repeatable and comma-separated. Plain names select top-level keys;
    /// dotted paths (parent.author) traverse objects; JSON Pointers (/parent/body) handle unusual keys.
    #[arg(
        long = "field",
        alias = "fields",
        value_delimiter = ',',
        value_name = "FIELD"
    )]
    pub(crate) fields: Vec<String>,
    /// Omit the header row.
    #[arg(long)]
    pub(crate) no_header: bool,
    /// JSONL input files. Omit when using --spool, or combine with --spool to append extra files.
    #[arg(num_args = 0.., value_name = "INPUTS")]
    pub(crate) inputs: Vec<PathBuf>,
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
    /// Stop after approximately N matching records have been scanned/emitted.
    /// With file_concurrency >1, already-running workers may emit a bounded over-shoot.
    #[arg(long, visible_alias = "head")]
    pub(crate) limit: Option<u64>,
    /// Resume by reusing per-source per-month matched-record checkpoints under
    /// `--work-dir` when the query/config/corpus fingerprint still matches.
    #[arg(long)]
    pub(crate) resume: bool,
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
    /// Resume by reusing per-source per-month matched-record checkpoints under
    /// `--work-dir` when the query/config/corpus fingerprint still matches.
    #[arg(long)]
    pub(crate) resume: bool,
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
    /// Required for csv/tsv; missing fields render as empty cells.
    /// Comments use `body`/`parent_id`/`link_id`; submissions use `title`/`selftext`/`domain`.
    #[arg(long, value_delimiter = ',')]
    pub(crate) whitelist: Vec<String>,
    /// Error if `--whitelist` matches zero fields in the first sampled records.
    #[arg(long)]
    pub(crate) strict_whitelist: bool,
    /// Convert `created_utc` to RFC3339 strings on JSON-family exports (not csv/tsv).
    #[arg(long)]
    pub(crate) human_timestamps: bool,
    /// Stop after approximately N records have been emitted.
    /// With file_concurrency >1, already-running workers may emit a bounded over-shoot.
    #[arg(long, visible_alias = "head")]
    pub(crate) limit: Option<u64>,
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
    /// Resume a prior JSON-family export with the same query/config/corpus (not csv/tsv).
    /// `jsonl`/`json` reuse per-month `.part_*.jsonl` files and `_progress.json`
    /// in a namespaced scratch dir under `--work-dir`; `spool`, `zst`, and
    /// `partitioned-jsonl` use `_progress.json` under `--out`. Changing filters,
    /// corpus paths, sources, date range, whitelist, timestamp formatting, or
    /// (for ZST) zst level invalidates the checkpoint and rebuilds the parts.
    #[arg(long)]
    pub(crate) resume: bool,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
pub(crate) enum ExportFmt {
    /// Single stitched `.jsonl` file (one record per line).
    Jsonl,
    /// Single `.json` file containing a JSON array of records.
    Json,
    /// Single RFC4180-style CSV file (requires --whitelist).
    Csv,
    /// Single tab-separated file (requires --whitelist; tabs and line breaks in values are rejected).
    Tsv,
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
    /// Resume by reusing per-source per-month matched-record checkpoints under
    /// `--work-dir` when the query/config/corpus fingerprint still matches.
    #[arg(long)]
    pub(crate) resume: bool,
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

    /// Before zstd validation, compare the requested --start/--end/--source against a corpus manifest.
    #[arg(long)]
    pub(crate) expected: bool,

    /// Custom corpus manifest JSON. Implies --expected; omitted means RETL's built-in manifest.
    #[arg(long = "manifest")]
    pub(crate) manifest: Option<PathBuf>,

    /// With --expected/--manifest, compute SHA-256 for present files that have manifest checksums.
    /// This may read multi-GB files in addition to the zstd integrity pass.
    #[arg(long)]
    pub(crate) verify_checksums: bool,

    /// Hidden compatibility trap: integrity is intentionally not resumable.
    #[arg(long, hide = true)]
    pub(crate) resume: bool,
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

    /// Do not write aggregate provenance manifest sidecars.
    #[arg(long)]
    pub(crate) no_manifest: bool,
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
    /// Directory used for per-run aggregate shard namespaces (default:
    /// alongside `--out`).
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
    /// Allow exponent notation for inexact floating grouped metrics. This is a
    /// display-style toggle, not a precision toggle: exact integer results stay
    /// exact decimal, and default decimal output no longer rounds to six places.
    #[arg(long)]
    pub(crate) scientific: bool,
    /// Hidden compatibility trap: aggregate reduces materialized inputs and is
    /// intentionally not resumable.
    #[arg(long, hide = true)]
    pub(crate) resume: bool,
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
    /// format. `export`, `scan`, `dedupe`, `count`, and `first-seen` also
    /// support `--resume`; `aggregate` and `integrity` intentionally do not.
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
    /// Do not write parents provenance manifest sidecars.
    #[arg(long)]
    pub(crate) no_manifest: bool,
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
    /// Resume by reusing per-source per-month matched-record checkpoints under
    /// `--work-dir` when the query/config/corpus fingerprint still matches.
    #[arg(long)]
    pub(crate) resume: bool,
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
    fn timestamp_flags_accept_epoch_rfc3339_minute_and_date_forms() {
        assert_eq!(parse_timestamp_bound("1606737600").unwrap(), 1_606_737_600);
        assert_eq!(
            parse_timestamp_bound("2020-11-30T12:00:00Z").unwrap(),
            1_606_737_600
        );
        assert_eq!(
            parse_timestamp_bound("2020-11-30T12:00Z").unwrap(),
            1_606_737_600
        );
        assert_eq!(parse_timestamp_bound("2020-12-01").unwrap(), 1_606_780_800);
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
