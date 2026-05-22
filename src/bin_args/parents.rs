use clap::{Args, ValueEnum};
use retl::YearMonth;
use std::path::PathBuf;

use super::MonitorOpts;

#[derive(ValueEnum, Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ParentIdKindArg {
    /// Bare IDs are comment IDs (`t1_...`).
    #[value(alias = "t1", alias = "comments", alias = "rc")]
    Comment,
    /// Bare IDs are submission IDs (`t3_...`).
    #[value(alias = "t3", alias = "submissions", alias = "rs")]
    Submission,
}

#[derive(Args, Debug)]
pub(crate) struct ParentsArgs {
    /// Spool directory containing `part_RC_YYYY-MM.jsonl` /
    /// `part_RS_YYYY-MM.jsonl` files produced by `retl export --format spool`.
    /// Omit when using `--ids-file` / `--parent-id` direct-ID mode.
    #[arg(long)]
    pub(crate) spool: Option<PathBuf>,
    /// Text file containing one parent ID per line. IDs may be prefixed
    /// (`t1_...`/`t3_...`) or bare when paired with `--id-kind`.
    #[arg(long = "ids-file", value_name = "PATH")]
    pub(crate) ids_file: Vec<PathBuf>,
    /// Parent ID to resolve directly; repeat for small lists. IDs may be
    /// prefixed (`t1_...`/`t3_...`) or bare when paired with `--id-kind`.
    #[arg(long = "parent-id", value_name = "ID")]
    pub(crate) parent_id: Vec<String>,
    /// Kind assigned to bare IDs from `--ids-file` / `--parent-id`.
    /// Prefixed `t1_...` / `t3_...` IDs do not need this flag.
    #[arg(
        long = "id-kind",
        alias = "parent-id-kind",
        value_enum,
        value_name = "KIND"
    )]
    pub(crate) id_kind: Option<ParentIdKindArg>,
    /// Optional inclusive start month (YYYY-MM) for direct-ID resolution.
    /// Spool mode infers the range from spool filenames plus `--window-months`.
    #[arg(long, value_name = "YYYY-MM")]
    pub(crate) start: Option<YearMonth>,
    /// Optional inclusive end month (YYYY-MM) for direct-ID resolution.
    /// Spool mode infers the range from spool filenames plus `--window-months`.
    #[arg(long, value_name = "YYYY-MM")]
    pub(crate) end: Option<YearMonth>,
    /// Cache directory for resolved parent shards.
    #[arg(long)]
    pub(crate) cache: PathBuf,
    /// Spool mode: output directory for attached spool files. Direct-ID mode:
    /// JSONL file receiving resolved parent payloads.
    #[arg(long, short)]
    pub(crate) out: PathBuf,
    /// Resume the parents pipeline by reusing matching cache shards. In spool
    /// mode, also skip already-attached output files only when their attach
    /// fingerprint still matches the spool file, parent cache, resolution
    /// window, and attach format. Direct-ID JSONL output is atomically
    /// rewritten. `export`, `scan`, `dedupe`, `count`, and `first-seen` also
    /// support `--resume`; `aggregate` and `integrity` intentionally do not.
    #[arg(long)]
    pub(crate) resume: bool,
    /// Spool mode only: months of slack added on each side of the spool's
    /// date range when scanning the corpus to resolve parent payloads
    /// (default 3). Rejected in `--ids-file`/`--parent-id` direct-ID mode,
    /// which scopes its scan with `--start`/`--end` instead.
    #[arg(long, value_name = "MONTHS")]
    pub(crate) window_months: Option<u32>,
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
    /// Observability / monitoring flags (opt-in, off by default). `parents`
    /// resolves parent payloads over a multi-month corpus window and can
    /// balloon the parent cache to several times the compressed input size,
    /// so `--events`/`--status-file`/`--max-rss-mb` are worth wiring up.
    #[command(flatten)]
    pub(crate) monitor: MonitorOpts,
}
