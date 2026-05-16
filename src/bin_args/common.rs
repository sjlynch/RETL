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
