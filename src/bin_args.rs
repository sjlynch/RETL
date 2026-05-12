//! CLI argument structs for the `retl` binary.
//!
//! `Cli` / `Command` describe the subcommand surface; `CommonOpts` plus the
//! per-subcommand `*Args` structs back the clap derive. These types are
//! binary-only — keep them out of `src/lib.rs`.

use clap::{Args, Parser, Subcommand, ValueEnum};
use retl::{Sources, YearMonth};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "retl",
    version,
    about = "Reddit ETL toolkit — scan, export, count, validate, and aggregate Reddit RC/RS .zst dumps.",
    long_about = None,
)]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Subcommand, Debug)]
pub(crate) enum Command {
    /// Scan and emit unique usernames matching the query selection.
    Scan(ScanArgs),
    /// Emit distinct keys (author, subreddit, or JSON pointer) matching the query selection.
    #[command(alias = "unique", alias = "distinct")]
    Dedupe(DedupeArgs),
    /// Export filtered records as JSONL, a JSON array, or per-month spool files.
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
// Common flags shared by all subcommands.
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

    /// Number of Rayon worker threads (defaults to the global pool).
    #[arg(long)]
    pub(crate) parallelism: Option<usize>,

    /// Number of monthly files processed concurrently.
    #[arg(long)]
    pub(crate) file_concurrency: Option<usize>,

    /// Disable progress bars.
    #[arg(long)]
    pub(crate) no_progress: bool,

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

    /// Source selection: rc (comments), rs (submissions), or both.
    #[arg(long, value_enum, default_value_t = SourceArg::Both)]
    pub(crate) source: SourceArg,

    /// Subreddit name (repeat for multiple). If none given, all subreddits match.
    #[arg(long = "subreddit", short = 's')]
    pub(crate) subreddits: Vec<String>,

    /// Include pseudo-users that are excluded by default: [deleted], [removed], and empty authors.
    #[arg(long = "include-deleted", alias = "include-pseudo-users")]
    pub(crate) include_deleted: bool,

    /// Inflight bytes budget for bucketing/dedupe producer/consumer pairs.
    /// 0 disables the explicit cap and falls back to memory-fraction sampling.
    #[arg(long)]
    pub(crate) inflight_bytes: Option<usize>,
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
pub(crate) struct ScanArgs {
    #[command(flatten)]
    pub(crate) common: CommonOpts,
    /// Output file for usernames (default: stdout).
    #[arg(long, short)]
    pub(crate) out: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub(crate) struct DedupeArgs {
    #[command(flatten)]
    pub(crate) common: CommonOpts,
    /// Key to deduplicate: `author`, `subreddit`, or `json:/pointer`.
    #[arg(long, value_name = "KEY")]
    pub(crate) key: String,
    /// Output text file, one unique key per line. Use `-` for stdout.
    #[arg(long, short)]
    pub(crate) out: PathBuf,
}

#[derive(Args, Debug)]
pub(crate) struct ExportArgs {
    #[command(flatten)]
    pub(crate) common: CommonOpts,
    /// Output format.
    #[arg(long, value_enum, default_value_t = ExportFmt::Jsonl)]
    pub(crate) format: ExportFmt,
    /// Output destination — file for `jsonl`/`json` (use `-` for stdout),
    /// directory for `spool`.
    #[arg(long, short)]
    pub(crate) out: PathBuf,
    /// Pretty-print the JSON array (only with `--format json`).
    #[arg(long)]
    pub(crate) pretty: bool,
    /// zstd compression level for `.zst` outputs. Clamped to 1..=22 by the library.
    #[arg(long)]
    pub(crate) zst_level: Option<i32>,
    /// Resume a prior export with the same query/config. `jsonl`/`json` reuse
    /// per-month `.part_*.jsonl` files and `_progress.json` in `--work-dir`;
    /// `spool` reuses part files and `_progress.json` in `--out`. Changing
    /// filters, sources, date range, whitelist, or timestamp formatting
    /// invalidates the checkpoint and rebuilds the parts.
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
}

#[derive(Args, Debug)]
pub(crate) struct CountArgs {
    #[command(flatten)]
    pub(crate) common: CommonOpts,
    /// Count mode: per month (`month`) or per author (`author`, writes TSV).
    #[arg(long, value_enum, default_value_t = CountMode::Month)]
    pub(crate) mode: CountMode,
    /// Output file (default stdout for `month`, required for `author`).
    /// Pass `-` to stream to stdout when `--mode month`.
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
    /// Bytes (decompressed) to sample per file in quick mode.
    #[arg(long, default_value_t = 64 * 1024)]
    pub(crate) sample_bytes: u64,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
pub(crate) enum IntegrityModeArg {
    Quick,
    Full,
}

#[derive(Args, Debug)]
pub(crate) struct AggregateArgs {
    #[command(flatten)]
    pub(crate) common: CommonOpts,
    /// JSONL input files to aggregate.
    #[arg(required = true, num_args = 1..)]
    pub(crate) inputs: Vec<PathBuf>,
    /// Output path. Without `--by`, writes JSON record-count state; with `--by`, writes TSV.
    #[arg(long, short)]
    pub(crate) out: PathBuf,
    /// Directory used for per-input aggregate shards (default: alongside `--out`).
    #[arg(long)]
    pub(crate) shards_dir: Option<PathBuf>,
    /// Pretty-print the final JSON (only used when `--by` is omitted).
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
    /// already-attached output files. `export` is the other CLI subcommand that
    /// supports `--resume`; aggregate/count/scan/integrity/first-seen do not.
    #[arg(long)]
    pub(crate) resume: bool,
    /// Months of slack added on each side of the spool's date range when
    /// scanning the corpus to resolve parent payloads.
    #[arg(long, default_value_t = 3)]
    pub(crate) window_months: u32,
    /// Path to corpus base dir (containing `comments/` and `submissions/`).
    #[arg(long, default_value = "./data")]
    pub(crate) data_dir: PathBuf,
    /// Scratch directory for sharded writers and stitched intermediates.
    #[arg(long, default_value = "./etl_work")]
    pub(crate) work_dir: PathBuf,
    /// Number of Rayon worker threads (defaults to the global pool).
    #[arg(long)]
    pub(crate) parallelism: Option<usize>,
    /// Number of monthly files processed concurrently.
    #[arg(long)]
    pub(crate) file_concurrency: Option<usize>,
    /// Disable progress bars.
    #[arg(long)]
    pub(crate) no_progress: bool,
    /// Inflight bytes budget for bucketing/dedupe producer/consumer pairs.
    #[arg(long)]
    pub(crate) inflight_bytes: Option<usize>,
}

#[derive(Args, Debug)]
pub(crate) struct FirstSeenArgs {
    #[command(flatten)]
    pub(crate) common: CommonOpts,
    /// Output TSV file: `<author>\t<earliest_created_utc>` per line.
    #[arg(long, short)]
    pub(crate) out: PathBuf,
}
