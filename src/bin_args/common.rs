use clap::{Args, ValueEnum};
use retl::{Sources, YearMonth};
use std::path::PathBuf;

use super::parsers::parse_timestamp_bound;

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

    /// Subreddit name (repeat for multiple). If none given, all subreddits match. Names are
    /// normalized to lowercase with any leading `r/` stripped before matching, so `Bitcoin`,
    /// `bitcoin`, and `r/Bitcoin` all match the same records. Blank values are rejected.
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

    /// Observability / monitoring flags (opt-in, off by default).
    #[command(flatten)]
    pub(crate) monitor: MonitorOpts,
}

/// Opt-in observability flags shared by every monitorable subcommand.
///
/// Kept separate from [`CommonOpts`] so commands with their own argument
/// shape (`parents`, `aggregate`) can flatten the monitoring surface without
/// also inheriting the corpus/query flags. See `docs/monitoring.md`.
#[derive(Args, Debug, Clone)]
pub(crate) struct MonitorOpts {
    /// Write a structured NDJSON event stream to this path (one JSON object
    /// per line). See docs/monitoring.md for the schema. Lets external
    /// watchers (LLM monitors, scripts) follow a long run without scraping
    /// human-formatted stderr.
    #[arg(long, value_name = "PATH")]
    pub(crate) events: Option<PathBuf>,

    /// Atomically rewrite a single JSON snapshot to this path every ~1 s.
    /// Contains pid, elapsed time, current/peak RSS, throttle state,
    /// counters, and the watchdog configuration. See
    /// `docs/monitoring.md` (`retl.status.v1` schema).
    #[arg(long, value_name = "PATH")]
    pub(crate) status_file: Option<PathBuf>,

    /// Watchdog cap on resident set size (mebibytes). Cross at or above and
    /// the process emits a `watchdog.rss_exceeded` event and exits with
    /// code 2. Atomic-write contract guarantees no corrupt outputs on
    /// abrupt exit.
    #[arg(long, value_name = "N")]
    pub(crate) max_rss_mb: Option<u64>,

    /// Watchdog cap on wall-clock runtime (seconds). Exceed → exit code 2,
    /// preceded by a `watchdog.runtime_exceeded` event.
    #[arg(long, value_name = "N")]
    pub(crate) max_runtime_sec: Option<u64>,

    /// Stop-file kill switch: the watchdog polls this path once per second
    /// and gracefully exits (code 0) if it appears. Useful when sending
    /// signals to a child process is awkward.
    #[arg(long, value_name = "PATH")]
    pub(crate) stop_file: Option<PathBuf>,

    /// Stderr log format. `RETL_LOG_FORMAT=json|text` env var overrides
    /// this flag. JSON output is one event per line, mirrors what
    /// `--events` writes to the file.
    #[arg(long, value_name = "FMT", default_value = "text")]
    pub(crate) log_format: String,

    /// Periodic `kind=status` mirror events emitted on the event stream,
    /// in seconds. 0 disables. Default 5.
    #[arg(long, value_name = "N", default_value_t = 5)]
    pub(crate) heartbeat_sec: u64,
}

#[derive(Args, Debug, Clone)]
pub(crate) struct QueryOpts {
    /// Record ID allow-list entry (repeatable). Accepts bare IDs plus t1_/t3_ fullnames; prefixed IDs constrain comment/submission matching. Blank or duplicate IDs are rejected.
    #[arg(long = "id", value_name = "ID")]
    pub(crate) ids: Vec<String>,

    /// Newline-delimited record IDs to include. Blank lines and lines beginning with # are ignored; inline comments are not stripped. Repeatable.
    #[arg(long = "ids-file", value_name = "PATH")]
    pub(crate) ids_files: Vec<PathBuf>,

    /// Author allow-list entry (repeatable). `--author-in` is an alias. Match is
    /// case-insensitive (record author lowercased before comparison). Blank values rejected.
    #[arg(long = "author", visible_alias = "author-in", value_name = "NAME")]
    pub(crate) authors: Vec<String>,

    /// Author deny-list entry (repeatable). Case-insensitive. Blank values are rejected.
    #[arg(long = "exclude-author", value_name = "NAME")]
    pub(crate) exclude_authors: Vec<String>,

    /// Exclude RETL's curated bot/service-account list (see `docs/default_bots.md` for the
    /// exact list) plus any names from `ETL_EXCLUDE_AUTHORS` (comma/semicolon/whitespace
    /// separated) and `ETL_EXCLUDE_AUTHORS_FILE` (newline-separated). Env-supplied names
    /// are additive — they merge with the defaults, not replace them.
    #[arg(long = "exclude-common-bots")]
    pub(crate) exclude_common_bots: bool,

    /// Regex matched against author names (case-sensitive — anchor or use `(?i)` for
    /// case-insensitive). Compiled before any data is read; invalid patterns are rejected.
    #[arg(long = "author-regex", value_name = "REGEX")]
    pub(crate) author_regex: Option<String>,

    /// Substring keyword filter, OR-joined across repetitions. Matched case-insensitively
    /// in comment `body`, submission `selftext`, and submission `title` (all three fields
    /// are searched on every record). Substring — not word-boundary — so `--keyword cat`
    /// matches `concatenate`. Blank values are rejected.
    #[arg(long = "keyword", value_name = "TEXT")]
    pub(crate) keywords: Vec<String>,

    /// Substring keyword filter, AND-joined across repetitions: every entry must appear
    /// (case-insensitively) somewhere across the same three fields (`body` / `selftext` /
    /// `title`). Blank values are rejected.
    #[arg(long = "keyword-all", value_name = "TEXT")]
    pub(crate) keywords_all: Vec<String>,

    /// Reject records whose `body`, `selftext`, or `title` contains this substring
    /// (case-insensitive). Repeatable; any single match drops the record. Blank rejected.
    #[arg(long = "exclude-keyword", value_name = "TEXT")]
    pub(crate) exclude_keywords: Vec<String>,

    /// Regex matched against comment `body`, submission `selftext`, and submission `title`
    /// (any of the three matching keeps the record). Invalid or blank patterns are rejected
    /// before scanning.
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

    /// Keep records with an http(s) URL anywhere in `body` / `selftext` / `title`, OR
    /// (submissions only) whose `url` field starts with `http`. Matches both text URLs
    /// and link-post URLs. To require a *link-post specifically*, combine with
    /// `--json /is_self=false` (or use `--domain`). See `docs/query_cookbook.md`.
    #[arg(long = "contains-url")]
    pub(crate) contains_url: bool,

    /// Inverse of `--contains-url`. Keeps only records with no http(s) URL anywhere in
    /// text fields and (for submissions) no http(s) `url` field. Mutually exclusive
    /// with `--contains-url` at runtime.
    #[arg(long = "no-url")]
    pub(crate) no_url: bool,

    /// Submission `domain` allow-list (repeatable). Submissions only — comments have no
    /// `domain` field and are silently dropped from the match set even when
    /// `--source both`. Blank values are rejected. Match is case-insensitive exact equality;
    /// for suffix/regex matches use `--json /domain~=REGEX`.
    #[arg(long = "domain", value_name = "DOMAIN")]
    pub(crate) domains: Vec<String>,

    /// Full-record JSON Pointer predicate (RFC 6901). Repeatable; multiple `--json` flags
    /// are AND-joined. Operators (LHS is the pointer, RHS is a value/regex):
    ///
    ///   exists:/path        — true iff `/path` resolves to any value (including null).
    ///   /path=value         — true iff `/path` equals `value`. Missing field → false.
    ///   /path!=value        — true iff `/path` resolves AND is unequal. Missing → false.
    ///   /path>N, >=N, <N, <=N — numeric, on finite numbers only. Missing → false.
    ///   /path~=REGEX        — regex match. Pointer must be a string. Missing → false.
    ///
    /// IMPORTANT: every operator except `exists:` evaluates to false when the field is
    /// absent — records lacking the field are dropped, not included. To keep records
    /// that may or may not have the field, gate the predicate with `exists:` first
    /// or move the check to `--source rs` only. Values use JSON scalars when possible
    /// (`true`, `false`, `null`, numbers, or quoted strings); otherwise treated as strings.
    /// See `docs/query_cookbook.md` for worked examples.
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
